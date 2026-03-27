"""Remote DuckDB wrapper — Arrow IPC over HTTP with QueryStation auth."""
from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from data_consumers._auth import QueryStationAuth

logger = logging.getLogger(__name__)


class RemoteDuckDBWrapper:
    """
    Dev helper that sends SQL to a remote DuckDB server
    and returns Polars DataFrames via Arrow IPC.

    Same .sql() / .show_tables() / .close() surface as DuckDBWrapper,
    plus .catalog(), .describe(), .export(), .run_sql_folder().
    """

    def __init__(
        self,
        api_key: str | None = None,
        auth_url: str | None = None,
    ) -> None:
        self._auth = QueryStationAuth(api_key=api_key, auth_url=auth_url)

    # ── core query ──────────────────────────────────────────

    def sql(self, query: str) -> pl.DataFrame:
        """Execute SQL on the remote DuckDB server, return a Polars DataFrame."""
        import httpx
        import pyarrow as pa

        token = self._auth.get_token()
        remote_url = self._auth.remote_url

        r = httpx.post(
            remote_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"type": "arrow", "sql": query},
            timeout=30,
        )

        if r.status_code == 401:
            logger.debug("Got 401, refreshing token and retrying")
            self._auth.force_refresh()
            token = self._auth.get_token()
            r = httpx.post(
                remote_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"type": "arrow", "sql": query},
                timeout=30,
            )

        r.raise_for_status()
        table = pa.ipc.open_stream(r.content).read_all()
        return pl.from_arrow(table)

    # ── catalog discovery ───────────────────────────────────

    def fetch_catalog(self) -> dict:
        """Fetch catalog metadata from the /catalog JSON endpoint.

        Returns the raw JSON dict with a "tables" key containing
        list of {catalog, schema, name, columns, columnNames, columnTypes}.
        """
        import httpx

        token = self._auth.get_token()
        remote_url = self._auth.remote_url
        catalog_url = f"{remote_url.rstrip('/')}/catalog"

        r = httpx.get(
            catalog_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )

        if r.status_code == 401:
            logger.debug("Got 401 on /catalog, refreshing token and retrying")
            self._auth.force_refresh()
            token = self._auth.get_token()
            catalog_url = f"{self._auth.remote_url.rstrip('/')}/catalog"
            r = httpx.get(
                catalog_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )

        r.raise_for_status()
        return r.json()

    def catalog(self) -> pl.DataFrame:
        """Return a DataFrame of all catalogs, schemas, and tables."""
        payload = self.fetch_catalog()
        tables = payload.get("tables", [])
        if not tables:
            return pl.DataFrame({"catalog": [], "schema": [], "name": [], "columns": []})
        df = pl.DataFrame(tables)
        if "catalog" not in df.columns:
            return pl.DataFrame({"catalog": [], "schema": [], "name": [], "columns": []})
        return df.select("catalog", "schema", "name", "columns")

    def describe(self, table: str) -> pl.DataFrame:
        """Return column names and types for a table via the /catalog endpoint."""
        db, schema, name = self._parse_table_ref(table)
        payload = self.fetch_catalog()
        match = next(
            (
                item for item in payload.get("tables", [])
                if item.get("catalog") == db
                and item.get("schema") == schema
                and item.get("name") == name
            ),
            None,
        )
        if not match:
            return pl.DataFrame({"column_name": [], "column_type": []})

        col_names = match.get("columnNames", [])
        col_types = match.get("columnTypes", [])
        return pl.DataFrame({"column_name": col_names, "column_type": col_types})

    def show_tables(self) -> None:
        """Print a summary of all remote tables to stdout."""
        from rich import box
        from rich.console import Console
        from rich.table import Table

        df = self.catalog()
        if df.is_empty():
            Console().print("[dim]No tables found.[/]")
            return

        t = Table(
            title="Remote DuckDB Tables",
            title_style="bold bright_yellow",
            header_style="bold bright_white",
            box=box.ROUNDED,
            border_style="bright_black",
        )
        t.add_column("catalog", style="bright_cyan")
        t.add_column("schema", style="bright_cyan")
        t.add_column("name", style="bold white")
        t.add_column("columns", justify="right", style="bright_green")
        for row in df.iter_rows():
            t.add_row(*[str(v) for v in row])
        Console().print(t)

    # ── export ──────────────────────────────────────────────

    @staticmethod
    def export(
        df: pl.DataFrame,
        file_name: str,
        file_type: str = "csv",
        output_dir: str = "data/exports",
    ) -> Path:
        """Export a DataFrame to parquet, csv, or json. Returns the output path."""
        full_path = Path(output_dir) / f"{file_name}.{file_type}"
        full_path.parent.mkdir(parents=True, exist_ok=True)

        if file_type == "parquet":
            df.write_parquet(str(full_path))
        elif file_type == "csv":
            df.write_csv(str(full_path))
        elif file_type == "json":
            df.write_ndjson(str(full_path))
        else:
            raise ValueError(f"Unknown export format: {file_type}")

        return full_path

    # ── batch SQL ───────────────────────────────────────────

    def run_sql_folder(
        self,
        folder: str | Path,
        export_fmt: str = "parquet",
        output_dir: str = "data/exports",
    ) -> dict[str, pl.DataFrame]:
        """Run all .sql files in a folder. Returns {name: DataFrame}.

        Each result is also exported to output_dir.
        """
        sql_dir = Path(folder)
        if not sql_dir.is_dir():
            raise FileNotFoundError(f"Not a directory: {folder}")

        results: dict[str, pl.DataFrame] = {}
        for sql_path in sorted(sql_dir.glob("*.sql")):
            query_text = sql_path.read_text().strip()
            if not query_text:
                continue
            df = self.sql(query_text)
            self.export(df, sql_path.stem, export_fmt, output_dir)
            results[sql_path.stem] = df

        return results

    # ── lifecycle ───────────────────────────────────────────

    def close(self) -> None:
        """No-op for remote (stateless HTTP). Present for interface parity."""

    # ── internal ────────────────────────────────────────────

    @staticmethod
    def _parse_table_ref(table: str) -> tuple[str, str, str]:
        """Parse 'catalog.schema.name' into (catalog, schema, name)."""
        parts = table.rsplit(".", 2)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return "lake", parts[0], parts[1]
        return "lake", "main", parts[0]
