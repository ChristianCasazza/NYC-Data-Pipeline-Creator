"""Factory for creating partitioned QueryStation-backed SQL assets.

Counterpart to ``discover_sql_assets`` for cases that need a ``PartitionsDefinition``
— keeps the YAML frontmatter surface small and delegates partitioning to
ordinary Dagster objects.

Usage::

    from opendata_framework.dagster import (
        create_querystation_sql_asset,
        yearly_partitions,
    )

    mta_ridership_yearly = create_querystation_sql_asset(
        name="mta_ridership_yearly",
        sql='''
            SELECT extract(year FROM date)::INT AS year, mode, sum(count) AS total
            FROM lake.nys_transportation.mta_daily_ridership
            WHERE date >= {{partition_start}} AND date < {{partition_end}}
            GROUP BY 1, 2
        ''',
        partitions_def=yearly_partitions("2020", end_offset=1),
        group="nys__transportation",
    )
"""
from __future__ import annotations

from typing import Any

import duckdb
import dagster as dg
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    AutomationCondition,
    Backoff,
    MaterializeResult,
    MetadataValue,
    PartitionsDefinition,
    RetryPolicy,
)

from opendata_framework.core.sql.runner_querystation import render_sql

_QUERYSTATION_RETRY = RetryPolicy(max_retries=3, delay=30, backoff=Backoff.EXPONENTIAL)


def create_querystation_sql_asset(
    name: str,
    sql: str,
    *,
    partitions_def: PartitionsDefinition | None = None,
    group: str = "Analytics",
    tags: dict[str, str] | None = None,
    description: str = "",
    deps: list[str] | None = None,
    automation_condition: AutomationCondition | None = None,
) -> list[Any]:
    """Create a QueryStation-backed SQL asset plus a row-count check.

    Args:
        name: Asset key. Becomes the parquet directory under ``data/clean/``.
        sql: SQL query. May reference ``{{partition_start}}``, ``{{partition_end}}``,
            or ``{{partition_key}}`` when the asset is partitioned.
        partitions_def: Optional Dagster partitions definition. If set, each
            partition materialization fires a separately-templated remote query
            and writes its own Hive-layout parquet (``year=YYYY/...``).
        group: Dagster UI group name.
        tags: Extra tags merged with ``{stage: analytics, source: querystation}``.
        description: Human-readable description.
        deps: Scheduling-only Dagster deps (asset names). Not SQL-parsed —
            remote table references don't resolve to local assets.
        automation_condition: Optional override; defaults to eager.

    Returns:
        ``[asset, check]`` — both must be registered at module scope so
        Dagster's component scanner picks them up.
    """
    merged_tags: dict[str, str] = {
        "stage": "analytics",
        "source": "querystation",
    }
    if tags:
        merged_tags.update(tags)

    static_md = {
        "sql": MetadataValue.md(f"```sql\n{sql}\n```"),
        "execution_mode": "querystation_remote",
    }

    @dg.asset(
        name=name,
        description=description,
        group_name=group,
        kinds={"querystation", "remote", "sql"},
        io_manager_key="analytics_io_manager",
        deps=[AssetKey(d) for d in (deps or [])],
        required_resource_keys={"querystation"},
        partitions_def=partitions_def,
        tags=merged_tags,
        metadata=static_md,
        retry_policy=_QUERYSTATION_RETRY,
        automation_condition=automation_condition or AutomationCondition.eager(),
    )
    def _asset(context) -> MaterializeResult:
        op = context.op_execution_context
        partition_key: str | None = None
        partition_start = None
        partition_end = None
        if op.has_partition_key:
            partition_key = op.partition_key
            if partitions_def is not None:
                try:
                    window = op.partition_time_window
                    partition_start = window.start
                    partition_end = window.end
                except Exception as exc:
                    # Non-time partitions: render_sql will raise if the SQL
                    # references {{partition_start}} etc. — the right behavior.
                    context.log.debug(f"No partition_time_window for {partition_key}: {exc}")

        rendered = render_sql(
            sql,
            partition_key=partition_key,
            partition_start=partition_start,
            partition_end=partition_end,
        )

        context.log.info(
            f"[QueryStation] {name}"
            f"{' partition=' + partition_key if partition_key else ''} "
            f"({len(rendered)} chars)"
        )
        df = context.resources.querystation.query(rendered)

        return MaterializeResult(
            value=df,
            metadata={
                "row_count": df.height,
                "columns": df.width,
                "partition_key": partition_key or "",
                "rendered_sql": MetadataValue.md(f"```sql\n{rendered}\n```"),
            },
        )

    check_name = f"{name}_row_count_check"

    @dg.asset_check(
        name=check_name,
        asset=AssetKey(name),
        blocking=True,
        required_resource_keys={"analytics_io_manager"},
        description="Fails if 0 rows are produced across any materialized partition.",
    )
    def _row_count_check(context) -> AssetCheckResult:
        io_manager = context.resources.analytics_io_manager
        glob_pattern = io_manager.get_glob_pattern(AssetKey(name), recursive=True)
        asset_root = io_manager.get_path_for_asset(AssetKey(name))

        if not asset_root.parent.exists():
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"row_count": 0, "reason": "No parquet files written."},
            )

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
        except Exception as exc:
            return AssetCheckResult(
                passed=False,
                severity=AssetCheckSeverity.ERROR,
                metadata={"reason": f"Query failed: {exc}"},
            )
        finally:
            con.close()

        return AssetCheckResult(passed=cnt > 0, metadata={"row_count": int(cnt)})

    return [_asset, _row_count_check]
