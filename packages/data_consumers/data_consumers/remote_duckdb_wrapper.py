"""Remote DuckDB wrapper — Arrow IPC over HTTP with QueryStation auth."""
from __future__ import annotations

import logging
import re
from pathlib import Path

import polars as pl

from data_consumers._auth import QueryStationAuth

logger = logging.getLogger(__name__)

# Identifier guard for any value we splice into SQL.
_SQL_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _check_ident(name: str, field: str) -> None:
    """Reject identifiers that would break SQL interpolation."""
    if not _SQL_IDENT.match(name):
        raise ValueError(
            f"{field}={name!r} is not a valid SQL identifier "
            "(allowed: letters, digits, underscore; must start with letter/underscore)"
        )


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

    def describe(
        self,
        table: str,
        *,
        with_comments: bool = False,
    ) -> pl.DataFrame:
        """Return column names and types for a table.

        With ``with_comments=True``, also left-joins per-column comments from
        the catalog (extra ``comment`` column, NULL where unset).
        """
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
            base = pl.DataFrame({"column_name": [], "column_type": []})
            return base.with_columns(pl.lit(None).alias("comment")) if with_comments else base

        df = pl.DataFrame({
            "column_name": match.get("columnNames", []),
            "column_type": match.get("columnTypes", []),
        })
        if not with_comments:
            return df
        comments = self.column_comments(table)
        return df.join(comments, on="column_name", how="left")

    # ── Comments — DuckDB introspection ─────────────────────
    #
    # DuckDB exposes table/column comments via ``duckdb_tables()`` and
    # ``duckdb_columns()``, which surface DuckLake's underlying metadata
    # (the ``__ducklake_metadata_<catalog>`` schema) through a stable SQL
    # API. Querying the metadata schema directly returns 500 from the
    # QueryStation gateway — these introspection functions are the route
    # that actually works through the public endpoint.

    def table_comments(
        self,
        schema: str | None = None,
        *,
        catalog: str = "lake",
        include_empty: bool = False,
    ) -> pl.DataFrame:
        """Return table-level comments visible to the gateway.

        Args:
            schema: Optional schema filter (e.g. ``'nyc_checkbook'``). ``None``
                returns every schema in the catalog.
            catalog: Catalog/database to look in. Defaults to ``'lake'`` (the
                QueryStation curated catalog). Pass another DB name to look
                elsewhere.
            include_empty: When True, include tables that have no comment
                (``comment`` will be NULL). Default False — only tables with
                an actual comment string appear.

        Returns:
            DataFrame with columns ``schema_name``, ``table_name``, ``comment``.
        """
        _check_ident(catalog, "catalog")
        where = [f"database_name = '{catalog}'"]
        if schema is not None:
            _check_ident(schema, "schema")
            where.append(f"schema_name = '{schema}'")
        if not include_empty:
            where.append("comment IS NOT NULL")
            where.append("comment != ''")
        sql = (
            "SELECT schema_name, table_name, comment "
            "FROM duckdb_tables() "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY schema_name, table_name"
        )
        return self.sql(sql)

    def column_comments(
        self,
        table: str,
        *,
        include_empty: bool = True,
    ) -> pl.DataFrame:
        """Return per-column comments for one table.

        Args:
            table: ``'schema.table'`` or ``'catalog.schema.table'``. When the
                catalog is omitted, defaults to ``'lake'`` (matches
                ``_parse_table_ref``).
            include_empty: When True (default), every declared column appears
                — columns without a comment have NULL. When False, only
                commented columns are returned.

        Returns:
            DataFrame with columns ``column_name``, ``comment``.
        """
        catalog, schema_name, table_name = self._parse_table_ref(table)
        _check_ident(catalog, "catalog")
        _check_ident(schema_name, "schema")
        _check_ident(table_name, "table")
        where = [
            f"database_name = '{catalog}'",
            f"schema_name = '{schema_name}'",
            f"table_name = '{table_name}'",
        ]
        if not include_empty:
            where.append("comment IS NOT NULL")
            where.append("comment != ''")
        sql = (
            "SELECT column_name, comment "
            "FROM duckdb_columns() "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY column_index"
        )
        return self.sql(sql)

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
