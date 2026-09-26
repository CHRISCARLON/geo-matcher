"""Integration test: runs a real join through a real SedonaContext against tiny
synthetic GeoParquet files built by the prepare() pipeline.
"""

import logging
import pathlib

import geopandas as gpd
import pyarrow as pa
import pytest
import sedona.db
import shapely.wkb
from shapely.geometry import LineString, Point, box

from geo_matcher.config import DatasetConfig, OgrSource, UsrnSource
from geo_matcher.join import (
    _DEFAULT_MODE,
    _run_in_batches,
    configure_sedona_session,
    run_usrn_polygon_join,
)
from geo_matcher.prepare import prepare

pytestmark = pytest.mark.integration

N_IN_POLY = 20
N_OUT_POLY = 30


@pytest.fixture
def usrns_parquet(tmp_path: pathlib.Path) -> pathlib.Path:
    """50 USRN centrelines, prepared via the real DuckDB pipeline: usrns 1-20 sit
    inside soil_config's polygon, usrns 21-50 sit far outside it."""
    in_poly = [LineString([(i * 100, 0), (i * 100 + 50, 0)]) for i in range(N_IN_POLY)]
    out_poly = [
        LineString([(100_000 + i * 100, 0), (100_000 + i * 100 + 50, 0)])
        for i in range(N_OUT_POLY)
    ]
    geometries = in_poly + out_poly

    gdf = gpd.GeoDataFrame(
        {
            "usrn": range(1, len(geometries) + 1),
            "street_type": ["Numbered Street"] * len(geometries),
            "geometry": geometries,
        },
        crs="EPSG:27700",
    )
    gpkg = tmp_path / "usrns.gpkg"
    gdf.to_file(str(gpkg), driver="GPKG")

    out = tmp_path / "usrns_27700.parquet"
    prepare(
        DatasetConfig(name="usrns", source=UsrnSource(path=gpkg), parquet_path=out),
        force=True,
    )
    return out


@pytest.fixture
def soil_config(tmp_path: pathlib.Path) -> DatasetConfig:
    """Two polygons — one covering usrns 1-20's centrelines, one nowhere near any USRN."""
    gdf = gpd.GeoDataFrame(
        {
            "musid": [101, 102],
            "geometry": [
                box(-10, -10, N_IN_POLY * 100, 10),
                box(1000, 1000, 1100, 1010),
            ],
        },
        crs="EPSG:27700",
    )
    gpkg = tmp_path / "soil.gpkg"
    gdf.to_file(str(gpkg), driver="GPKG")

    out = tmp_path / "soil_27700.parquet"
    cfg = DatasetConfig(name="soil", source=OgrSource(path=gpkg), parquet_path=out)
    prepare(cfg, force=True)
    return cfg


def test_run_usrn_polygon_join_matches_real_overlap(
    usrns_parquet: pathlib.Path, soil_config: DatasetConfig
):
    """A real SedonaContext should find exactly the 20 (USRN, polygon) pairs where
    musid 101 overlaps a centreline — usrns 1-20. usrns 21-50 and musid 102 sit
    nowhere near any polygon/USRN respectively, so they produce no matches."""
    sd = sedona.db.connect()
    configure_sedona_session(sd, target_partitions=2)

    result = run_usrn_polygon_join(sd, usrns_parquet, soil_config, mode=_DEFAULT_MODE)

    assert result.column_names == ["usrn", "street_type", "musid"]
    assert result.column("usrn").to_pylist() == list(range(1, N_IN_POLY + 1))
    assert set(result.column("musid").to_pylist()) == {101}
    assert len(result) == N_IN_POLY


# ---------------------------------------------------------------------------
# rows_per_batch — _run_in_batches batching
# ---------------------------------------------------------------------------


def test_run_in_batches_honors_custom_rows_per_batch(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A custom ``rows_per_batch`` must control how many batches ``_run_in_batches``
    splits *features* into — 5 rows / rows_per_batch=2 → ceil(5/2) = 3 batches.

    Exercises ``_run_in_batches`` directly (not through a full line join) with a
    trivial one-row USRN view and a huge search radius, so every batch trivially
    matches — this test is about batch mechanics, not spatial correctness.
    """
    sd = sedona.db.connect()
    configure_sedona_session(sd, target_partitions=2)
    sd.sql(
        "SELECT 1 AS usrn, ST_SetSRID(ST_Point(0.0, 0.0), 27700) AS geometry"
    ).to_view("usrns", overwrite=True)

    n = 5
    features = pa.table(
        {
            "asset_id": pa.array([f"F{i}" for i in range(n)]),
            "geometry": pa.array(
                [shapely.wkb.dumps(Point(float(i), 0.0)) for i in range(n)]
            ),
            "bbox": pa.array(
                [
                    {"xmin": float(i), "ymin": 0.0, "xmax": float(i), "ymax": 0.0}
                    for i in range(n)
                ]
            ),
        }
    )
    template = (
        "SELECT s.asset_id, u.usrn FROM usrns AS u "
        "JOIN test_rhs AS s ON ST_DWithin(u.geometry, s.geometry, 1000000) "
        "WHERE TRUE {spatial_filter}"
    )

    with caplog.at_level(logging.INFO, logger="geo_matcher"):
        parts = _run_in_batches(
            sd,
            features,
            "test_rhs",
            template,
            expand_m=1_000_000.0,
            post_process=lambda t: t,
            explain=False,
            label="Test phase",
            rows_per_batch=2,
        )

    assert "Test phase: 5 unmatched rows → 3 batches" in caplog.text
    assert sum(len(p) for p in parts) == n
