# opendata_framework/core/duckdb_utils.py
# Provides utility functions for creating and managing DuckDB connections.
# Refactored to require explicit warehouse paths instead of global state.
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import duckdb
import polars as pl
from duckdb import CatalogException


def connect(warehouse_path: str | None = None, *, read_only: bool = True, attach_warehouse: bool = True) -> duckdb.DuckDBPyConnection:
    """
    Create a DuckDB connection. 
    If attach_warehouse=True, warehouse_path must be provided.
    Attaches the on-disk warehouse as schema 'wh' and sets search_path='main,wh'.
    """
    con = duckdb.connect(database=":memory:", read_only=False)
    if attach_warehouse:
        if not warehouse_path:
            raise ValueError("warehouse_path is required when attach_warehouse=True")
        _attach_warehouse(con, warehouse_path, read_only=read_only)
        con.execute("SET search_path='main,wh';")
    return con


def _attach_warehouse(con: duckdb.DuckDBPyConnection, warehouse_path: str, *, read_only: bool = True) -> None:
    ro = " (READ_ONLY)" if read_only else ""
    con.execute(f"ATTACH '{warehouse_path}' AS wh{ro};")


@contextmanager
def session(warehouse_path: str | None = None, *, read_only: bool = True, attach_wh: bool = True) -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Context manager that yields a DuckDB connection.
    """
    con = connect(warehouse_path, read_only=read_only, attach_warehouse=attach_wh)
    try:
        yield con
    finally:
        con.close()


def query_df(sql: str, params: dict[str, Any] | tuple[Any, ...] | None = None, warehouse_path: str | None = None) -> pl.DataFrame:
    """
    Run a SQL query against an ephemeral DuckDB session.
    Returns a Polars DataFrame.
    """
    with session(warehouse_path=warehouse_path, attach_wh=bool(warehouse_path)) as con:
        if params:
            return con.execute(sql, params).pl()
        return con.execute(sql).pl()


def read_parquet_via_duckdb(
    path_or_glob: str,
    *,
    hive_partitioning: bool = False,
    union_by_name: bool = True,
) -> pl.DataFrame:
    """
    Read (possibly many) Parquet files using DuckDB's reader and return a Polars DF.
    Useful for deep globs and hive directories. Does not attach warehouse.
    """
    with session(attach_wh=False) as con:
        hive = "true" if hive_partitioning else "false"
        union = "true" if union_by_name else "false"
        sql = f"""
        SELECT * FROM read_parquet(
          '{path_or_glob}',
          hive_partitioning = {hive},
          union_by_name     = {union}
        );
        """
        return con.execute(sql).pl()


def ensure_view_from_warehouse(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    *,
    source_name: str | None = None,
) -> None:
    """
    Ensure a TEMP VIEW exists in the current connection for a given table/view
    from the attached 'wh' schema. Safe to call repeatedly.

    view_name: logical name to create in the current connection (quoted).
    source_name: optional different name inside 'wh'; defaults to view_name.
    """
    q_view = '"' + view_name.replace('"', '""') + '"'
    source = source_name or view_name
    q_src = '"' + source.replace('"', '""') + '"'
    try:
        con.execute(f"CREATE OR REPLACE TEMP VIEW {q_view} AS SELECT * FROM wh.{q_src};")
    except CatalogException:
        # If missing in wh, create an empty stub to keep downstream SQL compilable
        con.execute(f"CREATE OR REPLACE TEMP VIEW {q_view} AS SELECT NULL LIMIT 0;")


__all__ = [
    "connect",
    "session",
    "query_df",
    "read_parquet_via_duckdb",
    "ensure_view_from_warehouse",
]