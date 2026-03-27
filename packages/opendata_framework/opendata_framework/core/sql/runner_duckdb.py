# opendata_framework/core/sql/runner_duckdb.py
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import duckdb
import polars as pl
from dagster import AssetKey, get_dagster_logger

if TYPE_CHECKING:
    from opendata_framework.dagster.resources.io.polars_parquet_io_manager import PolarsParquetIOManager


def _is_extension_available(con: duckdb.DuckDBPyConnection, ext: str) -> bool:
    """Check if a DuckDB extension is installed and available."""
    result = con.execute(
        "SELECT installed FROM duckdb_extensions() WHERE extension_name = ?",
        [ext],
    ).fetchone()
    return result is not None and result[0]


def _try_resolve_dependency(
    dep: str,
    mgr: PolarsParquetIOManager,
    con: duckdb.DuckDBPyConnection,
    logger: Any,
) -> bool:
    """
    Attempt to resolve a dependency from a specific IO manager.
    Returns True if successful, False if this manager cannot provide the dependency.
    """
    asset_key = AssetKey(dep)

    # LBYL: Check the asset root directory, not the single-file path.
    # get_path_for_asset() returns the flat-file path (asset/asset.parquet),
    # which doesn't exist for hive-partitioned assets that only have
    # subdirectories (e.g. year=2025/). Checking the directory is correct
    # for both partitioned and unpartitioned layouts.
    if not mgr._base_path.exists():
        return False

    asset_root = mgr._base_path / asset_key.path[-1]
    if not asset_root.exists():
        return False

    glob_pattern = mgr.get_glob_pattern(asset_key, recursive=True)
    query = f"""
    CREATE OR REPLACE TEMP VIEW "{dep}" AS
    SELECT * FROM parquet_scan(
        '{glob_pattern}',
        hive_partitioning=true,
        union_by_name=true
    );
    """
    con.execute(query)
    logger.debug(f"Mounted JIT view for '{dep}' from {glob_pattern}")
    return True


def run_sql_in_duckdb(
    asset_name: str,
    sql: str,
    deps: Sequence[str],
    io_managers: list[PolarsParquetIOManager],
) -> pl.DataFrame:
    """
    Stateless Runner:
    1. Starts an ephemeral in-memory DuckDB session.
    2. Iterates over declared dependencies.
    3. Asks provided IO Managers for the storage location (glob) of each dependency.
    4. Registers a 'parquet_scan' view for found data (Just-In-Time binding).
    5. Executes the target SQL transformation.

    This removes the need for a persistent DuckDB warehouse file.
    """
    logger = get_dagster_logger()
    con = duckdb.connect(":memory:")

    # Enable object cache for performance
    con.execute("SET enable_object_cache=true;")

    # Load httpfs for S3/R2 support if available
    if _is_extension_available(con, "httpfs"):
        con.execute("LOAD httpfs;")
    elif _is_extension_available(con, "http"):
        # Fallback for older DuckDB versions
        con.execute("LOAD http;")
    # else: No remote file support - local paths only

    for dep in deps:
        resolved = False

        # Try to find the dependency in our prioritized list of IO Managers
        for mgr in io_managers:
            if _try_resolve_dependency(dep, mgr, con, logger):
                resolved = True
                break

        # Handle Missing Dependencies (Graceful Degradation)
        if not resolved:
            logger.warning(f"Could not locate data for dependency '{dep}'.")
            con.execute(f'CREATE OR REPLACE TEMP VIEW "{dep}" AS SELECT NULL WHERE 1=0;')

    # Execute the Transformation
    logger.info(f"Executing SQL for {asset_name}...")
    try:
        return con.execute(sql).pl()
    except duckdb.Error as e:
        logger.error(f"SQL Execution failed for {asset_name}: {e}")
        raise
    finally:
        con.close()
