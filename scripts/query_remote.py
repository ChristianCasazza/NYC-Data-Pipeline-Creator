#!/usr/bin/env python3
# Requires: run from workspace root with `uv run scripts/query_remote.py`
# Dependencies are satisfied by the workspace venv (data-consumers[remote], polars, rich, python-dotenv).
"""
Remote DuckDB ad-hoc query utility via QueryStation Arrow IPC API.

Thin CLI over data_consumers.RemoteDuckDBWrapper — all logic lives in the package.

Usage
-----
    uv run python scripts/query_remote.py --catalog
    uv run python scripts/query_remote.py --describe lake.nyc_operations.service_requests_311
    uv run python scripts/query_remote.py --sql "SELECT COUNT(*) FROM lake.nyc_operations.service_requests_311"
    uv run python scripts/query_remote.py "SELECT 1 AS hello"
    uv run python scripts/query_remote.py --sql "SELECT * FROM lake.nyc_finance.city_payroll LIMIT 10" --export payroll csv
    uv run python scripts/query_remote.py --run-sql-folder ./my_queries --export-format parquet

Expects QUERYSTATION_API_KEY in .env or environment.
Optionally set AUTH_URL (default: https://auth-dev.querystation.app).
"""
from __future__ import annotations

import argparse
import sys

import polars as pl
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.table import Table

from data_consumers import RemoteDuckDBWrapper

load_dotenv()

console = Console()


# ── display helpers (CLI-only presentation) ─────────────────

def _print_fancy_table(df: pl.DataFrame, title: str) -> None:
    """High-contrast rich table for metadata."""
    t = Table(
        title=title,
        title_style="bold bright_yellow",
        header_style="bold bright_white",
        box=box.ROUNDED,
        show_lines=True,
        border_style="bright_black",
    )
    for col in df.columns:
        t.add_column(col, style="bright_cyan", justify="left")
    for row in df.iter_rows():
        t.add_row(*[str(v) if v is not None else "" for v in row])
    console.print(t)


def _print_results_table(df: pl.DataFrame, limit: int) -> None:
    """Query results — rich table for narrow, polars for wide, vertical for single row."""
    console.print(f"\n[dim]{df.height} rows x {df.width} cols[/]\n")
    display = df.head(limit)

    if display.height == 1:
        t = Table(
            title="Result",
            title_style="bold bright_yellow",
            box=box.ROUNDED,
            show_lines=True,
            border_style="bright_black",
        )
        t.add_column("column", style="bold bright_cyan")
        t.add_column("value", style="white")
        row = display.row(0)
        for col, val in zip(display.columns, row):
            t.add_row(col, str(val) if val is not None else "")
        console.print(t)
        return

    if df.width <= 12:
        t = Table(
            title_style="bold bright_yellow",
            header_style="bold bright_white",
            box=box.SIMPLE,
            show_lines=False,
            border_style="bright_black",
        )
        for col in display.columns:
            t.add_column(col, style="bright_cyan", no_wrap=True, overflow="ellipsis", max_width=40)
        for row in display.iter_rows():
            t.add_row(*[str(v) if v is not None else "" for v in row])
        if df.height > limit:
            t.add_row(*[f"[dim]... {df.height - limit} more[/]" if i == 0 else "" for i in range(df.width)])
        console.print(t)
        return

    with pl.Config(tbl_rows=limit, tbl_cols=14, tbl_width_chars=console.width - 4):
        console.print(display)


# ── commands ────────────────────────────────────────────────

def cmd_catalog(db: RemoteDuckDBWrapper) -> int:
    df = db.catalog()
    if df.is_empty():
        console.print("\n[dim]No tables found.[/]")
        return 0

    catalogs = df.select("catalog").unique().sort("catalog")
    console.print("\n[bold]── Catalogs ──[/]")
    for row in catalogs.iter_rows():
        console.print(f"  {row[0]}")

    schemas = df.select("catalog", "schema").unique().sort("catalog", "schema")
    console.print("\n[bold]── Schemas ──[/]")
    for row in schemas.iter_rows():
        console.print(f"  {row[0]}.{row[1]}")

    console.print()
    _print_fancy_table(df, title="Remote Tables")
    return 0


def cmd_describe(db: RemoteDuckDBWrapper, table: str, with_comments: bool = False) -> int:
    df = db.describe(table, with_comments=with_comments)
    if df.is_empty():
        console.print(f"[red]Table not found:[/] {table}")
        return 1
    suffix = " · with comments" if with_comments else ""
    _print_fancy_table(df, title=f"Schema: {table} ({df.height} columns){suffix}")
    return 0


def cmd_table_comments(db: RemoteDuckDBWrapper, schema: str | None) -> int:
    df = db.table_comments(schema)
    if df.is_empty():
        scope = f"schema={schema}" if schema else "lake"
        console.print(f"[dim]No table comments found ({scope}).[/]")
        return 0
    title = f"Table comments ({df.height})" + (f" — schema={schema}" if schema else "")
    _print_fancy_table(df, title=title)
    return 0


def cmd_column_comments(db: RemoteDuckDBWrapper, table: str) -> int:
    df = db.column_comments(table)
    if df.is_empty():
        console.print(f"[red]Table not found:[/] {table}")
        return 1
    _print_fancy_table(df, title=f"Column comments: {table} ({df.height} columns)")
    return 0


def cmd_query(db: RemoteDuckDBWrapper, query: str, limit: int, export_name: str | None, export_fmt: str) -> int:
    df = db.sql(query)
    _print_results_table(df, limit)
    if export_name:
        path = db.export(df, export_name, export_fmt)
        console.print(f"[green]Exported[/] {df.height} rows to {path}")
    return 0


def cmd_sql_folder(db: RemoteDuckDBWrapper, folder: str, export_fmt: str) -> int:
    console.print(f"\n[bold]Running SQL files from {folder}[/]\n")
    try:
        results = db.run_sql_folder(folder, export_fmt)
    except FileNotFoundError as exc:
        console.print(f"[red]{exc}[/]")
        return 1
    for name, df in results.items():
        console.print(f"  [cyan]{name}[/]: {df.height} rows x {df.width} cols")
    if not results:
        console.print("[dim]No .sql files found.[/]")
    return 0


# ── main ────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Query remote DuckDB via QueryStation Arrow IPC API"
    )
    parser.add_argument("sql", nargs="?", default=None, help="SQL to execute")
    parser.add_argument("--sql", dest="sql_flag", help="SQL to execute (flag form)")
    parser.add_argument("--catalog", action="store_true", help="Show catalogs, schemas, and tables")
    parser.add_argument("--describe", metavar="TABLE", help="Show schema of a table")
    parser.add_argument("--with-comments", action="store_true",
                        help="When used with --describe, also include per-column comments from DuckLake metadata")
    parser.add_argument("--table-comments", nargs="?", const="__ALL__", metavar="SCHEMA",
                        help="Show table-level comments. Optionally filter to one schema (e.g. --table-comments nyc_checkbook)")
    parser.add_argument("--column-comments", metavar="TABLE",
                        help="Show per-column comments for a table")
    parser.add_argument("--limit", type=int, default=50, help="Max rows to print (default: 50)")
    parser.add_argument(
        "--export", nargs=2, metavar=("NAME", "FORMAT"),
        help="Export results: --export my_data csv  (formats: csv, parquet, json)",
    )
    parser.add_argument("--run-sql-folder", metavar="DIR", help="Run all .sql files in a folder and export results")
    parser.add_argument("--export-format", default="parquet", help="Format for --run-sql-folder exports (default: parquet)")
    args = parser.parse_args()
    query = args.sql_flag or args.sql

    db = RemoteDuckDBWrapper()

    try:
        if args.run_sql_folder:
            return cmd_sql_folder(db, args.run_sql_folder, args.export_format)
        if args.catalog:
            return cmd_catalog(db)
        if args.column_comments:
            return cmd_column_comments(db, args.column_comments)
        if args.table_comments:
            schema = None if args.table_comments == "__ALL__" else args.table_comments
            return cmd_table_comments(db, schema)
        if args.describe:
            return cmd_describe(db, args.describe, with_comments=args.with_comments)
        if query:
            export_name = args.export[0] if args.export else None
            export_fmt = args.export[1] if args.export else "csv"
            return cmd_query(db, query, args.limit, export_name, export_fmt)
        return cmd_catalog(db)
    except Exception as exc:
        console.print(f"[red]Error:[/] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
