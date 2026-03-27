# data_consumers/duckdb_wrapper.py
"""Dev helper to explore Parquet and run ad-hoc SQL against an in-memory DuckDB engine."""
from __future__ import annotations

import glob as _glob
import logging
import os
from pathlib import Path as _Path

import duckdb
import polars as pl
from rich.console import Console
from rich.table import Table


class DuckDBWrapper:
    """
    Dev helper to explore Parquet and run ad-hoc SQL against
    an in-memory DuckDB engine (with the on-disk warehouse attached).
    """

    def __init__(self, duckdb_path: str | None = None) -> None:
        if not duckdb_path:
            duckdb_path = os.getenv("WAREHOUSE_DB_PATH")
        if not duckdb_path:
            raise ValueError("WAREHOUSE_DB_PATH env var not set and no path provided.")

        self.con = duckdb.connect(database=":memory:", read_only=False)
        self.con.execute(f"ATTACH '{duckdb_path}' AS wh (READ_ONLY)")
        self.con.execute("SET search_path='main,wh'")

        try:
            self.con.execute("INSTALL httpfs;")
            self.con.execute("LOAD httpfs;")
        except Exception as e:
            logging.getLogger(__name__).debug("httpfs extension not available: %s", e)

        self.registered_tables: list[str] = []

    def register_data_view(self, paths: list[str], table_names: list[str]) -> None:
        if len(paths) != len(table_names):
            raise ValueError("paths and table_names must be same length")
        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            if not _glob.glob(path_str):
                print(f"Skipping {table_name}: no files found → {path_str}")
                continue
            suffix = _Path(path_str).suffix.lower()
            if suffix == ".parquet":
                q = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path_str}')"
            elif suffix == ".csv":
                q = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
            elif suffix == ".json":
                q = f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
            else:
                print(f"Skipping {table_name}: unsupported file type for {path_str}")
                continue
            self.con.execute(q)
            self.registered_tables.append(table_name)
            print(f"View '{table_name}' → {path_str}")

    def register_data_table(self, paths: list[str], table_names: list[str]) -> None:
        if len(paths) != len(table_names):
            raise ValueError("paths and table_names must be same length")
        for path, table_name in zip(paths, table_names):
            path_str = str(path)
            if not _glob.glob(path_str):
                print(f"Skipping {table_name}: no files found → {path_str}")
                continue
            suffix = _Path(path_str).suffix.lower()
            if suffix == ".parquet":
                q = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{path_str}')"
            elif suffix == ".csv":
                q = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{path_str}')"
            elif suffix == ".json":
                q = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{path_str}')"
            else:
                print(f"Skipping {table_name}: unsupported file type for {path_str}")
                continue
            self.con.execute(q)
            self.registered_tables.append(table_name)
            print(f"Table '{table_name}' → {path_str}")

    def register_partitioned_data_view(self, base_path: str, table_name: str, wildcard: str = "**/*.parquet") -> None:
        base = _Path(base_path)
        path_str = str(base / wildcard)
        if not _glob.glob(path_str, recursive=True):
            print(f"Skipping partitioned {table_name}: no .parquet matched → {path_str}")
            return
        self.con.execute(
            f"""
            CREATE OR REPLACE VIEW {table_name} AS
            SELECT * FROM read_parquet(
              '{path_str}',
              hive_partitioning=true,
              union_by_name=true
            )
            """
        )
        self.registered_tables.append(table_name)
        print(f"Partitioned view '{table_name}' → {path_str}")

    def show_tables(self) -> None:
        rows = self.con.execute("SELECT * FROM duckdb_tables() ORDER BY name").fetchall()
        t = Table(title="DuckDB Tables / Views")
        t.add_column("name"), t.add_column("schema"), t.add_column("type")
        for r in rows:
            _, schema_name, name, typ, _ = r
            t.add_row(name, schema_name, typ)
        Console().print(t)

    def sql(self, query: str) -> pl.DataFrame:
        return self.con.execute(query).pl()

    def close(self) -> None:
        self.con.close()
