# opendata_framework/dagster/assets/sql_assets.py
# Updated to use the Stateless JIT execution model.
# Removes all references to 'duckdb_warehouse' and persistent DB paths.
# Updated to support Domain-Driven Grouping via SQL frontmatter.

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from collections.abc import Sequence

import duckdb
import polars as pl
import dagster as dg
from dagster import AssetKey, AutomationCondition, MaterializeResult, MetadataValue

from opendata_framework.core.sql.discovery import discover_sql_specs
from opendata_framework.core.sql.parser import extract_table_names
from opendata_framework.core.sql.runner_duckdb import run_sql_in_duckdb


def _markdown_table(df: pl.DataFrame, rows: int = 5) -> str:
    head = df.head(rows)
    cols = head.columns
    md_lines: list[str] = [
        "| " + " | ".join(cols) + " |",
        "| " + " | ".join(["---"] * len(cols)) + " |",
    ]
    for row in head.iter_rows():
        md_lines.append("| " + " | ".join("" if x is None else str(x) for x in row) + " |")
    return "\n".join(md_lines)


def _build_asset_core(
    *,
    asset_name: str,
    sql: str,
    meta: dict[str, Any],
    extra_deps: Sequence[str] | None,
    group: str,
) -> list[tuple[str, Any]]:
    """
    Constructs a SQL asset. Uses DuckLake execution when ``source: ducklake``
    is set in frontmatter, otherwise stateless JIT over Parquet files.
    """
    is_ducklake = meta.get("source") == "ducklake"

    # 1. Calculate Dependencies
    declared = set(meta.get("deps", []))
    extras   = set(extra_deps or [])

    if is_ducklake:
        # DuckLake: deps are scheduling-only (no implicit SQL parsing).
        # The SQL references opendata_lake.schema.table directly.
        run_deps = sorted(declared | extras)
    else:
        # Parquet: implicit deps via SQL table-name parsing
        implicit = {t for t in extract_table_names(sql) if t != asset_name}
        run_deps = sorted(declared | extras | implicit)

    # 2. Define Dagster Deps (Upstream Assets only)
    dagster_deps = [AssetKey(k) for k in run_deps]

    static_md = {
        "sql": MetadataValue.md(f"```sql\n{sql}\n```"),
        "schema": MetadataValue.json(meta),
        "execution_mode": "ducklake" if is_ducklake else "stateless_jit",
    }
    tags = {str(k): str(v) for k, v in (meta.get("tags", {}) or {}).items()}

    # Resolve Group: Prefer frontmatter group, fallback to passed default
    assigned_group = meta.get("group", group)

    if is_ducklake:
        resource_keys = {"ducklake", "analytics_io_manager"}
        asset_kinds = {"ducklake", "sql", "parquet"}
    else:
        resource_keys = {"clean_io_manager", "analytics_io_manager", "raw_large_io_manager"}
        asset_kinds = {"duckdb", "sql", "parquet"}

    @dg.asset(
        name=asset_name,
        group_name=assigned_group,
        kinds=asset_kinds,
        io_manager_key="analytics_io_manager",
        deps=dagster_deps,
        required_resource_keys=resource_keys,
        metadata=static_md,
        tags=tags,
        automation_condition=AutomationCondition.eager(),
    )
    def _asset(context) -> MaterializeResult:
        if is_ducklake:
            from opendata_private.ducklake.runner import run_sql_in_ducklake

            df = run_sql_in_ducklake(
                asset_name=asset_name,
                sql=sql,
                ducklake_resource=context.resources.ducklake,
            )
        else:
            io_managers = [
                context.resources.analytics_io_manager,
                context.resources.clean_io_manager,
                context.resources.raw_large_io_manager
            ]
            df = run_sql_in_duckdb(
                asset_name=asset_name,
                sql=sql,
                deps=run_deps,
                io_managers=io_managers
            )

        context.log.debug(f"{asset_name}: deps={', '.join(run_deps) or '∅'} | rows={df.height}")

        return MaterializeResult(
            value=df,
            metadata={
                "row_count": df.height,
                "sample": MetadataValue.md(_markdown_table(df))
            }
        )

    check_name = f"{asset_name}_row_count_check"

    @dg.asset_check(
        name=check_name, 
        asset=AssetKey(asset_name), 
        blocking=True,
        required_resource_keys={"analytics_io_manager"},
        description="Fails if 0 rows are produced."
    )
    def _row_count_check(context) -> dg.AssetCheckResult:
        io_manager = context.resources.analytics_io_manager
        glob_pattern = io_manager.get_glob_pattern(AssetKey(asset_name), recursive=True)
        asset_root = io_manager.get_path_for_asset(AssetKey(asset_name))
        
        if not asset_root.exists():
             return dg.AssetCheckResult(
                passed=False,
                severity=dg.AssetCheckSeverity.ERROR,
                metadata={"row_count": 0, "reason": "No parquet files written."},
            )

        # Quick DuckDB check on the output
        con = duckdb.connect()
        try:
            cnt = con.execute(
                f"""
                SELECT COUNT(*) FROM parquet_scan(
                  '{glob_pattern}',
                  hive_partitioning=true,
                  union_by_name=true
                )
                """
            ).fetchone()[0]
        except Exception as e:
            return dg.AssetCheckResult(
                passed=False, 
                severity=dg.AssetCheckSeverity.ERROR, 
                metadata={"reason": f"Query failed: {str(e)}"}
            )

        return dg.AssetCheckResult(passed=cnt > 0, metadata={"row_count": int(cnt)})

    return [
        (asset_name, _asset),
        (check_name, _row_count_check),
    ]


def discover_sql_assets(
    *,
    root: Path,
    extra_deps: dict[str, Sequence[str]] | None = None,
    group: str = "Analytics",
) -> dict[str, Any]:
    """Scan ``root`` for *.sql files with YAML frontmatter and build Dagster assets.

    Each ``.sql`` file becomes a stateless DuckDB asset that reads dependencies
    from Parquet via JIT views. Dependencies are auto-detected from SQL table
    references and can be supplemented via ``extra_deps``.

    Args:
        root: Directory to scan recursively for ``.sql`` files.
        extra_deps: Per-asset dependency overrides ``{asset_name: [dep1, dep2]}``.
        group: Default Dagster group name (overridable per-asset via frontmatter).

    Returns:
        Dict mapping asset names to Dagster definitions. Merge into
        ``globals()`` for auto-discovery by Dagster's component scanner.
    """
    extra_deps = extra_deps or {}
    sql_specs = discover_sql_specs(root=root, extra_deps=extra_deps)
    known_assets = {spec.name for spec in sql_specs}

    registry: dict[str, Any] = {}
    for spec in sql_specs:
        generated_assets = _build_asset_core(
            asset_name=spec.name,
            sql=spec.sql,
            meta=spec.meta,
            extra_deps=list(spec.extra_deps),
            group=group,
        )

        for key, fn in generated_assets:
            if key in registry:
                raise RuntimeError(f"Duplicate definition detected: '{key}'")
            registry[key] = fn

    return registry