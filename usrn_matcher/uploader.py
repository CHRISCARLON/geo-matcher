from __future__ import annotations

import os
import pathlib
from typing import Callable, Protocol, TypeVar, runtime_checkable

import duckdb


# Define a proper Uploader type
@runtime_checkable
class Uploader(Protocol):
    """Defining an uploader type contract"""

    def upload(
        self,
        source: str | pathlib.Path,
        database: str,
        schema: str,
        table: str,
        replace: bool = True,
    ) -> None: ...


_registry: dict[str, Uploader] = {}

_U = TypeVar("_U", bound=Uploader)


# TODO: add this as a util somewhere as it gets repeated in join.py
def register(name: str) -> Callable[[type[_U]], type[_U]]:
    """Register a class as an Uploader type"""

    def decorator(cls: type[_U]) -> type[_U]:
        instance = cls()
        if not isinstance(instance, Uploader):
            raise TypeError(
                f"{cls.__name__} does not satisfy the Uploader protocol — "
                "must implement upload(source, database, schema, table, replace)"
            )
        _registry[name] = instance
        return cls

    return decorator


def get_uploader(name: str) -> Uploader:
    if name not in _registry:
        raise KeyError(
            f"No uploader registered for '{name}'. Available: {list(_registry)}"
        )
    return _registry[name]


def _motherduck_connection() -> duckdb.DuckDBPyConnection:
    token = os.environ.get("motherduck_token") or os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise ValueError(
            "MotherDuck token not found. Set the 'motherduck_token' environment variable."
        )
    return duckdb.connect("md:")


@register("parquet")
class ParquetUploader(Uploader):
    def upload(
        self,
        source: str | pathlib.Path,
        database: str,
        schema: str,
        table: str,
        replace: bool = True,
    ) -> None:
        path = pathlib.Path(source).resolve()
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        qualified = f"{database}.{schema}.{table}"
        verb = "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"

        con = _motherduck_connection()
        con.execute(f"{verb} {qualified} AS SELECT * FROM read_parquet('{path}')")

        result = con.sql(f"SELECT COUNT(*) FROM {qualified}").fetchone()
        row_count = result[0] if result else 0
        print(f"Uploaded {row_count:,} rows → {qualified}")


if __name__ == "__main__":
    get_uploader("parquet").upload(
        "matched_data/naptan_usrn.parquet",
        database="street_manager_data",
        schema="matched_data",
        table="naptan_usrn_test",
    )
