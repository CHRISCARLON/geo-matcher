"""Integration tests — full pipeline runs using real input files.

Requires files in input_data/:
    - osopenusrn.gpkg
    - soil.gpkg
    - Stops.csv

Skip automatically if those files are missing (e.g. in CI without data).
All tests are marked ``integration``.
"""

import pathlib
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from usrn_matcher import DatasetConfig, DTFConfig, UsrnMatcher
from usrn_matcher.bboxes import LEEDS
from usrn_matcher.dtf import (
    to_dtf_csv,
    to_dtf_flat_csv,
    to_dtf_geoparquet,
    to_dtf_gpkg,
)
from usrn_matcher.prepare import prepare_dataset, prepare_from_csv, prepare_usrns

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

INPUT = pathlib.Path("input_data")
USRN_GPKG = INPUT / "osopenusrn.gpkg"
SOIL_GPKG = INPUT / "soil.gpkg"
STOPS_CSV = INPUT / "Stops.csv"


def _skip_if_missing(*paths: pathlib.Path) -> None:
    for p in paths:
        if not p.exists():
            pytest.skip(f"Input file not found: {p}")


# ---------------------------------------------------------------------------
# Session-scoped fixtures — prepare once, reuse across all tests
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def cache_dir(tmp_path_factory) -> pathlib.Path:
    """Shared output_data directory for the session."""
    return tmp_path_factory.mktemp("cache")


@pytest.fixture(scope="session")
def usrn_parquet(cache_dir) -> pathlib.Path:
    _skip_if_missing(USRN_GPKG)
    out = cache_dir / "usrns_27700.parquet"
    prepare_usrns(USRN_GPKG, out)
    return out


@pytest.fixture(scope="session")
def soil_parquet(cache_dir) -> pathlib.Path:
    _skip_if_missing(SOIL_GPKG)
    cfg = DatasetConfig(
        name="soil",
        source_path=SOIL_GPKG,
        parquet_path=cache_dir / "soil_27700.parquet",
    )
    prepare_dataset(cfg)
    return cfg.parquet_path


@pytest.fixture(scope="session")
def stops_parquet(cache_dir) -> pathlib.Path:
    _skip_if_missing(STOPS_CSV)
    out = cache_dir / "stops_27700.parquet"
    prepare_from_csv(
        csv_path=STOPS_CSV,
        parquet_path=out,
        x_col="Easting",
        y_col="Northing",
    )
    return out


@pytest.fixture(scope="session")
def soil_matcher(usrn_parquet, soil_parquet, cache_dir) -> UsrnMatcher:
    cfg = DatasetConfig(
        name="soil",
        source_path=soil_parquet,
        parquet_path=soil_parquet,
    )
    return UsrnMatcher(usrn_parquet=usrn_parquet, rhs_config=cfg)


@pytest.fixture(scope="session")
def stops_matcher(usrn_parquet, stops_parquet) -> UsrnMatcher:
    cfg = DatasetConfig(
        name="stops",
        source_path=stops_parquet,
        parquet_path=stops_parquet,
        columns=["ATCOCode", "CommonName"],
    )
    return UsrnMatcher(usrn_parquet=usrn_parquet, rhs_config=cfg)


# ---------------------------------------------------------------------------
# Prepare phase
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_prepare_usrns_writes_geoparquet(usrn_parquet):
    assert usrn_parquet.exists()
    schema = pq.read_schema(str(usrn_parquet))
    assert "usrn" in schema.names
    assert "geometry" in schema.names
    assert "bbox" in schema.names


@pytest.mark.integration
def test_prepare_usrns_is_idempotent(usrn_parquet):
    """Second call with force=False must skip and return the same path."""
    result = prepare_usrns(USRN_GPKG, usrn_parquet, force=False)
    assert result == usrn_parquet


@pytest.mark.integration
def test_prepare_dataset_writes_geoparquet(soil_parquet):
    assert soil_parquet.exists()
    schema = pq.read_schema(str(soil_parquet))
    assert "geometry" in schema.names
    assert "bbox" in schema.names


@pytest.mark.integration
def test_prepare_csv_writes_geoparquet(stops_parquet):
    assert stops_parquet.exists()
    schema = pq.read_schema(str(stops_parquet))
    assert "geometry" in schema.names
    assert "bbox" in schema.names


@pytest.mark.integration
def test_prepare_geoparquet_has_covering_metadata(usrn_parquet):
    """GeoParquet 1.1 covering key must be present in the file metadata."""
    import json

    meta = pq.read_schema(str(usrn_parquet)).metadata
    geo = json.loads(meta[b"geo"])
    geom_col = geo["columns"]["geometry"]
    assert "covering" in geom_col, "GeoParquet 1.1 covering key missing"
    assert "bbox" in geom_col["covering"]


# ---------------------------------------------------------------------------
# Intersect join
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_intersect_join_returns_rows(soil_matcher):
    table = soil_matcher.match_intersect(bbox=LEEDS)
    assert isinstance(table, pa.Table)
    assert len(table) > 0


@pytest.mark.integration
def test_intersect_join_schema(soil_matcher):
    table = soil_matcher.match_intersect(bbox=LEEDS)
    assert "usrn" in table.schema.names
    assert "street_type" in table.schema.names
    assert "geometry" in table.schema.names


@pytest.mark.integration
def test_intersect_join_usrn_column_is_integer(soil_matcher):
    table = soil_matcher.match_intersect(bbox=LEEDS)
    assert pa.types.is_integer(table.schema.field("usrn").type)


@pytest.mark.integration
def test_intersect_join_include_rhs_geometry(soil_matcher):
    table = soil_matcher.match_intersect(bbox=LEEDS, include_rhs_geometry=True)
    assert "rhs_geometry" in table.schema.names
    # rhs_geometry should be non-null for at least some rows
    rhs_col = table.column("rhs_geometry")
    assert rhs_col.null_count < len(table)


# ---------------------------------------------------------------------------
# Nearest join
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_nearest_join_returns_rows(stops_matcher):
    table = stops_matcher.match_nearest(bbox=LEEDS, distance_m=50)
    assert isinstance(table, pa.Table)
    assert len(table) > 0


@pytest.mark.integration
def test_nearest_join_schema(stops_matcher):
    table = stops_matcher.match_nearest(bbox=LEEDS, distance_m=50)
    assert "usrn" in table.schema.names
    assert "distance_m" in table.schema.names
    assert "ATCOCode" in table.schema.names
    assert "CommonName" in table.schema.names


@pytest.mark.integration
def test_nearest_join_distances_are_positive(stops_matcher):
    table = stops_matcher.match_nearest(bbox=LEEDS, distance_m=50)
    distances = table.column("distance_m").to_pylist()
    assert all(d >= 0 for d in distances)


@pytest.mark.integration
def test_nearest_join_distances_within_radius(stops_matcher):
    radius = 25.0
    table = stops_matcher.match_nearest(bbox=LEEDS, distance_m=radius)
    distances = table.column("distance_m").to_pylist()
    assert all(d <= radius for d in distances), "Results outside search radius"


@pytest.mark.integration
def test_nearest_join_include_rhs_geometry(stops_matcher):
    table = stops_matcher.match_nearest(
        bbox=LEEDS, distance_m=50, include_rhs_geometry=True
    )
    assert "rhs_geometry" in table.schema.names


# ---------------------------------------------------------------------------
# DTF export
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def dtf_table(stops_matcher) -> pa.Table:
    """Nearest join result with rhs_geometry — used for all DTF export tests."""
    return stops_matcher.match_nearest(
        bbox=LEEDS, distance_m=50, include_rhs_geometry=True
    )


@pytest.fixture(scope="session")
def dtf_cfg() -> DTFConfig:
    return DTFConfig(swa_org_name="Test Council", swa_org_ref=9999, rhs_name="stops")


@pytest.mark.integration
def test_dtf_csv_written(dtf_table, dtf_cfg, tmp_path):
    out = tmp_path / "stops.csv"
    result = to_dtf_csv(dtf_table, dtf_cfg, out)
    assert result == out
    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert content.startswith("10,")  # type 10 header record
    assert "99," in content  # type 99 trailer record
    assert '"63a",' in content  # attribution records


@pytest.mark.integration
def test_dtf_csv_trailer_count_matches(dtf_table, dtf_cfg, tmp_path):
    """Trailer record count must equal type69 + 63a + 67a records."""
    out = tmp_path / "stops.csv"
    to_dtf_csv(dtf_table, dtf_cfg, out)
    lines = out.read_text(encoding="utf-8").strip().splitlines()
    trailer = lines[-1]
    declared_count = int(trailer.split(",")[1])
    actual_count = sum(
        1
        for line in lines
        if line.startswith("69,")
        or line.startswith('"63a",')
        or line.startswith('"67a",')
    )
    assert declared_count == actual_count


@pytest.mark.integration
def test_dtf_geoparquet_written(dtf_table, dtf_cfg, tmp_path):
    out = tmp_path / "stops.parquet"
    result = to_dtf_geoparquet(dtf_table, dtf_cfg, out)
    assert result == out
    assert out.exists()
    schema = pq.read_schema(str(out))
    assert "USRN" in schema.names  # DTF uses uppercase column names
    assert "geometry" in schema.names


@pytest.mark.integration
def test_dtf_flat_csv_written(dtf_table, dtf_cfg, tmp_path):
    import csv as _csv

    out = tmp_path / "stops_flat.csv"
    result = to_dtf_flat_csv(dtf_table, dtf_cfg, out)
    assert result == out
    assert out.exists()
    with open(out, newline="", encoding="utf-8") as f:
        rows = list(_csv.DictReader(f))
    assert len(rows) > 0
    assert "USRN" in rows[0]  # DTF uses uppercase column names
    assert "geometry" in rows[0]


@pytest.mark.integration
def test_dtf_gpkg_written(dtf_table, dtf_cfg, tmp_path):
    import geopandas as gpd

    out = tmp_path / "stops.gpkg"
    result = to_dtf_gpkg(dtf_table, dtf_cfg, out)
    assert result == out
    assert out.exists()
    layer_names = gpd.list_layers(str(out))["name"].tolist()
    assert "stops" in layer_names


# ---------------------------------------------------------------------------
# to_csv and to_parquet writers
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_to_csv_writes_wkt_geometry(soil_matcher, tmp_path):
    table = soil_matcher.match_intersect(bbox=LEEDS)
    out = tmp_path / "result.csv"
    soil_matcher.to_csv(table, out)
    assert out.exists()
    content = out.read_text(encoding="utf-8")
    assert "usrn" in content.splitlines()[0]
    assert "LINESTRING" in content or "MULTILINESTRING" in content or "POINT" in content


@pytest.mark.integration
def test_to_parquet_writes_file(soil_matcher, tmp_path):
    table = soil_matcher.match_intersect(bbox=LEEDS)
    out = tmp_path / "result.parquet"
    soil_matcher.to_parquet(table, out)
    assert out.exists()
    written = pq.read_table(str(out))
    assert len(written) == len(table)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_cli_prepare_and_match(tmp_path, monkeypatch):
    """End-to-end CLI: prepare → match → output file written."""
    _skip_if_missing(USRN_GPKG, SOIL_GPKG)

    cache_dir = tmp_path / "cache"
    matched_dir = tmp_path / "matched"
    cache_dir.mkdir()
    matched_dir.mkdir()

    # -- prepare --
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "usrn-matcher",
            "prepare",
            "--usrn-gpkg",
            str(USRN_GPKG),
            "--rhs-gpkg",
            str(SOIL_GPKG),
            "--rhs-name",
            "soil",
            "--cache-dir",
            str(cache_dir),
        ],
    )
    UsrnMatcher.cli()

    assert (cache_dir / "usrns_27700.parquet").exists()
    assert (cache_dir / "soil_27700.parquet").exists()

    # -- match --
    bbox = [str(x) for x in LEEDS]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "usrn-matcher",
            "match",
            "--rhs-name",
            "soil",
            "--cache-dir",
            str(cache_dir),
            "--matched-dir",
            str(matched_dir),
            "--bbox",
            *bbox,
            "--output",
            "csv",
        ],
    )
    UsrnMatcher.cli()

    output_file = matched_dir / "usrn_soil_attribution.csv"
    assert output_file.exists()
    assert output_file.stat().st_size > 0


@pytest.mark.integration
def test_cli_prepare_csv_and_match_nearest(tmp_path, monkeypatch):
    """End-to-end CLI: prepare-csv → match nearest → output file written."""
    _skip_if_missing(USRN_GPKG, STOPS_CSV)

    cache_dir = tmp_path / "cache"
    matched_dir = tmp_path / "matched"
    cache_dir.mkdir()
    matched_dir.mkdir()

    # -- prepare-csv --
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "usrn-matcher",
            "prepare-csv",
            "--csv",
            str(STOPS_CSV),
            "--name",
            "stops",
            "--cache-dir",
            str(cache_dir),
        ],
    )
    UsrnMatcher.cli()

    # also need USRNs prepared for the match step
    prepare_usrns(USRN_GPKG, cache_dir / "usrns_27700.parquet")

    # -- match nearest --
    bbox = [str(x) for x in LEEDS]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "usrn-matcher",
            "match",
            "--rhs-name",
            "stops",
            "--cache-dir",
            str(cache_dir),
            "--matched-dir",
            str(matched_dir),
            "--mode",
            "nearest",
            "--distance",
            "50",
            "--bbox",
            *bbox,
            "--output",
            "csv",
        ],
    )
    UsrnMatcher.cli()

    output_file = matched_dir / "usrn_stops_attribution.csv"
    assert output_file.exists()
    assert output_file.stat().st_size > 0
