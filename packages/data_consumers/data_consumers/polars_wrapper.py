# data_consumers/polars_wrapper.py
"""Dev helper around polars.SQLContext for ad-hoc SQL over Parquet/CSV/JSON."""
from __future__ import annotations

import glob as _glob
from collections.abc import Sequence
from pathlib import Path as _Path
from typing import Any

import polars as pl
from rich.console import Console
from rich.table import Table


class PolarsWrapper:
    """
    Dev helper around `polars.SQLContext`.
      - register single-file or Hive-partitioned datasets (lazy)
      - keep a catalogue -> `show_tables()`
      - run ad-hoc SQL with `.run_query(sql)`
      - pull a registered table as a LazyFrame via `.lazy(name)`
    """

    def __init__(self) -> None:
        self.ctx = pl.SQLContext()
        self._catalogue: dict[str, pl.LazyFrame] = {}

    def _register(self, name: str, lf: pl.LazyFrame) -> None:
        self.ctx.register(name, lf)
        self._catalogue[name] = lf

    def register_data_view(
        self,
        paths: Sequence[Any],
        table_names: Sequence[str],
    ) -> None:
        if len(paths) != len(table_names):
            raise ValueError("len(paths) must equal len(table_names)")
        for p, name in zip(paths, table_names):
            p = _Path(p)
            pattern = str(p)
            matches = _glob.glob(pattern)
            if not matches:
                print(f"Skipping {name} – no files for pattern {pattern}")
                continue
            ext = p.suffix.lower()
            if ext == ".parquet":
                lf = pl.scan_parquet(pattern)
            elif ext == ".csv":
                lf = pl.scan_csv(pattern)
            elif ext == ".json":
                lf = pl.scan_ndjson(pattern)
            else:
                raise ValueError(f"Unsupported file type: {ext}")
            self._register(name, lf)
            print(f"View '{name}' registered ({len(matches)} file(s))")

    def register_partitioned_data_view(
        self,
        base_path: Any,
        table_name: str,
        wildcard: str = "year=*/month=*/*.parquet",
    ) -> None:
        base_path = _Path(base_path)
        pattern = str(base_path / wildcard)
        if not _glob.glob(pattern):
            print(f"Skipping {table_name} – no parquet files for {pattern}")
            return
        lf = pl.scan_parquet(pattern, hive_partitioning=True)
        self._register(table_name, lf)
        print(f"Partitioned view '{table_name}' registered")

    def bulk_register_data(
        self,
        repo_root: Any,
        base_path: str,
        table_names: Sequence[str],
        wildcard: str = "*.parquet",
    ) -> None:
        repo_root = _Path(repo_root)
        paths = [repo_root / base_path / name / wildcard for name in table_names]
        self.register_data_view(paths, table_names)

    def bulk_register_partitioned_data(
        self,
        repo_root: Any,
        base_path: str,
        table_names: Sequence[str],
        wildcard: str = "year=*/month=*/*.parquet",
    ) -> None:
        repo_root = _Path(repo_root)
        for name in table_names:
            self.register_partitioned_data_view(
                base_path=repo_root / base_path / name,
                table_name=name,
                wildcard=wildcard,
            )

    def show_tables(self) -> None:
        table = Table(title="Polars registered views", show_lines=True)
        table.add_column("View name", style="bold yellow")
        table.add_column("Lazy ?", justify="center", style="cyan")
        for name, lf in self._catalogue.items():
            table.add_row(name, str(isinstance(lf, pl.LazyFrame)))
        Console().print(table)

    def run_query(
        self, sql: str, collect: bool = True
    ) -> pl.DataFrame | pl.LazyFrame:
        result = self.ctx.execute(sql)
        return result.collect() if collect else result

    def lazy(self, table_name: str) -> pl.LazyFrame:
        if table_name not in self._catalogue:
            raise KeyError(f"No table registered as '{table_name}'")
        return self._catalogue[table_name]
