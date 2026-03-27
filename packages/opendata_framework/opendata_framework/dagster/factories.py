# opendata_framework/dagster/factories.py
# High-level factories for generating standardized asset pipelines.
# Implements the "Schema-Driven" pattern for Socrata ingestion.
# 2-Stage Pipeline: Landing(CSV) -> Clean(Parquet).
# Landing stores gzipped CSV shards; clean reads them lazily, applies
# schema contract + enrichment, and writes typed Parquet in one streaming pass.
# Updated to support fine-grained organization with domain/scope grouping.

from __future__ import annotations

import warnings
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

import polars as pl
from dagster import (
    AutomationCondition,
    AssetDep,
    AssetSpec,
    AssetsDefinition,
    IdentityPartitionMapping,
    PartitionMapping,
    PartitionsDefinition,
    TimeWindowPartitionMapping,
    TimeWindowPartitionsDefinition,
)

from dagster import TableColumn
from opendata_framework.core.polars_utils import SchemaContract, apply_schema_contract
from opendata_framework.enrichments.builder import StandardEnrichments
from opendata_framework.core.schema.contracts import build_table_schema, build_table_schema_from_contract, normalize_schema
from opendata_framework.dagster.assets.checks import (
    define_cross_partition_summary_check,
    define_row_accounting_check,
)
from opendata_framework.dagster.assets.ingestors import (
    define_checkbook_landing_source,
    define_clean_asset,
    define_socrata_landing_source,
)
from opendata_framework.dagster.standards import CheckbookIngestConfig, SocrataIngestConfig

PartitionGrain = Literal["unpartitioned", "same_grain", "monthly_to_yearly"]


class PipelineResult(list):
    """Named container for factory-generated assets.

    Extends ``list`` for full backwards compatibility with Dagster's component
    scanner, which expects an iterable of ``AssetsDefinition`` at module level.
    Supports ``result[0]`` (landing), ``result[1]`` (clean), iteration, and
    ``len()``.  New code should prefer the named attributes
    ``result.landing``, ``result.clean``, etc.
    """

    def __init__(
        self,
        landing: AssetsDefinition,
        clean: AssetsDefinition,
        checks: list[AssetsDefinition] | None = None,
    ) -> None:
        items: list[AssetsDefinition] = [landing, clean]
        if checks:
            items.extend(checks)
        super().__init__(items)
        self.landing = landing
        self.clean = clean
        self.checks = checks or []


@dataclass(frozen=True)
class CleanDiagnosticsConfig:
    """Controls expensive diagnostics for factory-generated clean assets."""

    capture_output_schema: bool = True
    capture_transform_source: bool = True
    capture_row_counts: bool = False
    capture_dropped_rows: Literal["off", "sample", "full"] = "off"
    dropped_rows_csv_threshold: int = 20

    def __post_init__(self) -> None:
        allowed = {"off", "sample", "full"}
        if self.capture_dropped_rows not in allowed:
            msg = (
                "capture_dropped_rows must be one of "
                f"{sorted(allowed)}, got {self.capture_dropped_rows!r}"
            )
            raise ValueError(msg)
        if self.dropped_rows_csv_threshold < 1:
            raise ValueError("dropped_rows_csv_threshold must be >= 1")


def _build_group_name(
    group: str | None,
    domain: str | None,
    geographic_scope: str | None,
) -> str:
    """
    Build the group name using the hierarchical pattern.
    Priority: explicit group > scope__domain > domain > "ungrouped"
    """
    if group:
        return group
    if geographic_scope and domain:
        return f"{geographic_scope}__{domain}"
    if domain:
        return domain
    return "ungrouped"


def _infer_partition_grain(
    landing_partitions: PartitionsDefinition | None,
    clean_partitions: PartitionsDefinition | None,
) -> PartitionGrain:
    """Infer the partition grain relationship between landing and clean."""
    if landing_partitions is None:
        return "unpartitioned"
    if clean_partitions is None or clean_partitions is landing_partitions:
        return "same_grain"
    # If both are TimeWindow, check if landing is finer than clean
    if isinstance(landing_partitions, TimeWindowPartitionsDefinition) and isinstance(
        clean_partitions, TimeWindowPartitionsDefinition
    ):
        landing_fmt = landing_partitions.fmt
        clean_fmt = clean_partitions.fmt
        # Monthly landing ("%Y-%m") → Yearly clean ("%Y")
        if len(landing_fmt) > len(clean_fmt):
            return "monthly_to_yearly"
    return "same_grain"


def _build_standard_tags(
    stage: Literal["landing", "clean"],
    source: str,
    domain: str | None,
    geographic_scope: str | None,
    extra_tags: dict[str, str] | None = None,
) -> dict[str, str]:
    """Build standardized tags for cross-cutting categorization."""
    tags: dict[str, str] = {
        "stage": stage,
        "source": source,
    }
    if domain:
        tags["domain"] = domain
    if geographic_scope:
        tags["geographic_scope"] = geographic_scope
    if extra_tags:
        tags.update(extra_tags)
    return tags


def create_socrata_pipeline(
    name: str,
    socrata_config: SocrataIngestConfig,
    schema: SchemaContract,
    *,
    # Organization (NEW)
    domain: str | None = None,
    geographic_scope: str | None = None,
    group: str | None = None,  # Explicit override
    # Partitioning
    partitions_def: PartitionsDefinition | None = None,
    clean_partitions_def: PartitionsDefinition | None = None,
    partition_mapping: PartitionMapping | None = None,
    # Logic
    post_transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    enrichments: StandardEnrichments | None = None,
    # Metadata
    description: str = "",
    owner: str = "",
    url: str = "",
    extra_metadata: dict[str, Any] | None = None,
    # Data dictionary — derived columns appended to auto-built schema
    derived_columns: list[TableColumn] | None = None,
    # Scheduling
    cron_schedule: str | None = None,
    # Eager downstream propagation
    eager_downstream: bool = False,
    # Custom Tags
    tags: dict[str, str] | None = None,
    # Row accounting
    expect_filtered_rows: bool = False,
    diagnostics: CleanDiagnosticsConfig | None = None,
) -> PipelineResult:
    """Create a 2-stage Socrata pipeline: Landing (CSV) → Clean (Parquet).

    Minimal example::

        from opendata_framework.dagster import (
            create_socrata_pipeline, SocrataIngestConfig,
        )
        import polars as pl

        pipeline = create_socrata_pipeline(
            name="my_dataset",
            socrata_config=SocrataIngestConfig(endpoint="abcd-1234", time_col="date"),
            schema={
                "id": ("id", pl.Utf8, "Record ID"),
                "date": ("date", pl.Datetime, "Event date"),
            },
        )
        my_dataset = pipeline.clean  # Dagster asset

    Args:
        name: Asset key for the clean output. Landing key is ``{name}_landing``.
        socrata_config: Socrata API connection and partition filter settings.
        schema: Column mapping — 2-tuple ``{src: (dst, type)}`` or
            3-tuple ``{src: (dst, type, description)}`` (preferred; auto-builds
            the Dagster data dictionary).
        enrichments: Declarative enrichment config. Generates both the transform
            and derived column metadata. Runs before ``post_transform_fn``.
        post_transform_fn: Custom Polars transform applied after schema contract
            and enrichments. Use for domain-specific logic.
        expect_filtered_rows: Set True when the clean stage intentionally drops
            rows (e.g. NULL filtering). Suppresses row-count check failures.
        diagnostics: Controls expensive clean-stage diagnostics. Default mode
            keeps the clean stage lazy and relies on asset checks for row-drop
            visibility.

    Returns:
        PipelineResult with ``.landing``, ``.clean``, and ``.checks`` attributes.
        The result is also iterable for Dagster's component scanner.
    """
    # Normalize schema: accept 2-tuple or 3-tuple, extract contract for apply_schema_contract
    schema_contract, is_3tuple = normalize_schema(schema)
    diagnostics = diagnostics or CleanDiagnosticsConfig()

    # Resolve declarative enrichments into transform + derived columns
    enrichment_transform = None
    if enrichments is not None:
        enrichment_transform = enrichments.build_transform_fn()
        enrichment_cols = enrichments.build_derived_columns()
        derived_columns = [*enrichment_cols, *(derived_columns or [])]

    if enrichments is not None and extra_metadata and "dagster/column_schema" in extra_metadata:
        warnings.warn(
            f"{name}: extra_metadata contains 'dagster/column_schema' which "
            "overrides auto-generated enrichment columns in the data dictionary. "
            "Remove it from extra_metadata to use enrichment-derived schema.",
            stacklevel=2,
        )

    # Resolve group name
    effective_group = _build_group_name(group, domain, geographic_scope)

    # --- 1. Define Landing Asset (CSV shards) ---
    landing_key = f"{name}_landing"

    # Auto-detect filtering strategy if not explicitly set
    if partitions_def and socrata_config.partition_filter_type == "time":
        if not isinstance(partitions_def, TimeWindowPartitionsDefinition):
            socrata_config = socrata_config.model_copy(
                update={"partition_filter_type": "equality"}
            )

    landing_metadata: dict[str, Any] = {
        **socrata_config.to_metadata(),
        "dagster/io_manager_key": "landing_io_manager",
        "description": f"Landing Zone (CSV) for {name}",
    }
    if url:
        landing_metadata["source_url"] = url

    landing_tags = _build_standard_tags(
        stage="landing",
        source="socrata",
        domain=domain,
        geographic_scope=geographic_scope,
        extra_tags=tags,
    )

    landing_spec = AssetSpec(
        key=landing_key,
        description=f"Landing Zone (CSV) for {name}",
        group_name=effective_group,
        kinds={"socrata", "landing", "csv"},
        partitions_def=partitions_def,
        metadata=landing_metadata,
        tags=landing_tags,
        automation_condition=AutomationCondition.on_cron(cron_schedule) if cron_schedule else None,
    )

    landing_asset = define_socrata_landing_source(landing_spec)

    # --- 2. Determine Wiring for Clean Asset (depends on Landing) ---
    final_clean_partitions = clean_partitions_def or partitions_def

    clean_deps = []
    if partitions_def:
        if partition_mapping:
            mapping = partition_mapping
        elif socrata_config.partition_filter_type == "time":
            # When grain changes (e.g. monthly landing → yearly clean),
            # allow partial materialization so the current year can be
            # cleaned before all months have landed.
            allow_partial = clean_partitions_def is not None and clean_partitions_def is not partitions_def
            mapping = TimeWindowPartitionMapping(
                start_offset=0,
                end_offset=0,
                allow_nonexistent_upstream_partitions=allow_partial,
            )
        else:
            mapping = IdentityPartitionMapping()

        clean_deps.append(AssetDep(landing_key, partition_mapping=mapping))
    else:
        clean_deps.append(AssetDep(landing_key))

    # --- 3. Define Clean Asset Wrapper ---
    clean_metadata: dict[str, Any] = {
        "dagster/io_manager_key": "clean_large_io_manager",

        "description": description or f"Cleaned version of {name}",
        "data_owner": owner,
        "source_url": url,
    }
    if extra_metadata:
        clean_metadata.update(extra_metadata)

    # Auto-build data dictionary if not explicitly provided
    if "dagster/column_schema" not in clean_metadata:
        if is_3tuple:
            clean_metadata["dagster/column_schema"] = build_table_schema(
                schema, derived_columns=derived_columns,
            )
        else:
            clean_metadata["dagster/column_schema"] = build_table_schema_from_contract(
                schema_contract, derived_columns=derived_columns,
            )

    clean_tags = _build_standard_tags(
        stage="clean",
        source="socrata",
        domain=domain,
        geographic_scope=geographic_scope,
        extra_tags=tags,
    )

    clean_spec = AssetSpec(
        key=name,
        description=description or f"Cleaned version of {name}",
        group_name=effective_group,
        kinds={"polars", "clean"},
        partitions_def=final_clean_partitions,
        deps=clean_deps,
        metadata=clean_metadata,
        tags=clean_tags,
        automation_condition=AutomationCondition.eager() if eager_downstream else None,
    )

    # Build combined transform: schema contract → enrichments → custom logic
    combined_post = post_transform_fn
    if enrichment_transform and post_transform_fn:
        _enrich, _custom = enrichment_transform, post_transform_fn

        def combined_post(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _custom(_enrich(lf))
    elif enrichment_transform:
        combined_post = enrichment_transform

    def _pipeline_logic(lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = apply_schema_contract(lf, schema_contract)
        if combined_post:
            lf = combined_post(lf)
        return lf

    clean_asset = define_clean_asset(
        clean_spec,
        transform_fn=_pipeline_logic,
        schema_contract=schema_contract,
        post_transform_fn=combined_post,
        diagnostics=diagnostics,
    )

    # --- 4. Row Accounting Check ---
    grain = _infer_partition_grain(partitions_def, final_clean_partitions)
    checks: list[AssetsDefinition] = [
        define_row_accounting_check(
            clean_key=name,
            landing_key=landing_key,
            partition_grain=grain,
            allows_row_drop=expect_filtered_rows,
            clean_io_manager_key="clean_large_io_manager",
            partitions_def=final_clean_partitions,
        ),
    ]

    # --- 5. Cross-Partition Summary (partitioned assets only) ---
    if final_clean_partitions:
        checks.append(define_cross_partition_summary_check(
            clean_key=name,
            landing_key=landing_key,
            partition_grain=grain,
            clean_io_manager_key="clean_large_io_manager",
        ))

    return PipelineResult(
        landing=landing_asset,
        clean=clean_asset,
        checks=checks,
    )


def create_checkbook_pipeline(
    name: str,
    checkbook_config: CheckbookIngestConfig,
    schema: SchemaContract,
    *,
    # Organization
    domain: str | None = None,
    geographic_scope: str | None = None,
    group: str | None = None,
    # Partitioning
    partitions_def: PartitionsDefinition | None = None,
    clean_partitions_def: PartitionsDefinition | None = None,
    partition_mapping: PartitionMapping | None = None,
    # Logic
    post_transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    enrichments: StandardEnrichments | None = None,
    # Metadata
    description: str = "",
    owner: str = "",
    url: str = "",
    extra_metadata: dict[str, Any] | None = None,
    # Data dictionary — derived columns appended to auto-built schema
    derived_columns: list[TableColumn] | None = None,
    # Custom Tags
    tags: dict[str, str] | None = None,
    # Row accounting
    expect_filtered_rows: bool = False,
    diagnostics: CleanDiagnosticsConfig | None = None,
) -> PipelineResult:
    """Create a 2-stage Checkbook NYC pipeline: Landing (CSV) → Clean (Parquet).

    Minimal example::

        from opendata_framework.dagster import (
            create_checkbook_pipeline, CheckbookIngestConfig,
        )

        pipeline = create_checkbook_pipeline(
            name="my_checkbook_data",
            checkbook_config=CheckbookIngestConfig(
                type_of_data="Spending",
                response_columns=["agency", "amount", "issue_date"],
            ),
            schema=MY_SCHEMA,
        )
        my_asset = pipeline.clean

    Returns:
        PipelineResult with ``.landing``, ``.clean``, and ``.checks`` attributes.
    """
    # Normalize schema: accept 2-tuple or 3-tuple
    schema_contract, is_3tuple = normalize_schema(schema)
    diagnostics = diagnostics or CleanDiagnosticsConfig()

    # Resolve declarative enrichments
    enrichment_transform = None
    if enrichments is not None:
        enrichment_transform = enrichments.build_transform_fn()
        enrichment_cols = enrichments.build_derived_columns()
        derived_columns = [*enrichment_cols, *(derived_columns or [])]

    if enrichments is not None and extra_metadata and "dagster/column_schema" in extra_metadata:
        warnings.warn(
            f"{name}: extra_metadata contains 'dagster/column_schema' which "
            "overrides auto-generated enrichment columns in the data dictionary. "
            "Remove it from extra_metadata to use enrichment-derived schema.",
            stacklevel=2,
        )

    effective_group = _build_group_name(group, domain, geographic_scope)

    # --- 1. Landing Asset (CSV shards from XML API) ---
    landing_key = f"{name}_landing"

    landing_metadata: dict[str, Any] = {
        **checkbook_config.to_metadata(),
        "dagster/io_manager_key": "landing_io_manager",
        "description": f"Landing Zone (CSV) for {name}",
    }
    if url:
        landing_metadata["source_url"] = url

    landing_tags = _build_standard_tags(
        stage="landing",
        source="checkbook",
        domain=domain,
        geographic_scope=geographic_scope,
        extra_tags=tags,
    )

    landing_spec = AssetSpec(
        key=landing_key,
        description=f"Landing Zone (CSV) for {name}",
        group_name=effective_group,
        kinds={"checkbook", "landing", "csv"},
        partitions_def=partitions_def,
        metadata=landing_metadata,
        tags=landing_tags,
    )

    landing_asset = define_checkbook_landing_source(landing_spec)

    # --- 2. Clean Asset (depends on Landing) ---
    final_clean_partitions = clean_partitions_def or partitions_def

    clean_deps = []
    if partitions_def:
        if partition_mapping:
            mapping = partition_mapping
        elif clean_partitions_def is not None and clean_partitions_def is not partitions_def:
            # Grain change (e.g. monthly→yearly): allow partial materialization
            mapping = TimeWindowPartitionMapping(
                allow_nonexistent_upstream_partitions=True,
            )
        else:
            mapping = IdentityPartitionMapping()
        clean_deps.append(AssetDep(landing_key, partition_mapping=mapping))
    else:
        clean_deps.append(AssetDep(landing_key))

    clean_metadata: dict[str, Any] = {
        "dagster/io_manager_key": "clean_large_io_manager",

        "description": description or f"Cleaned version of {name}",
        "data_owner": owner,
        "source_url": url,
    }
    if extra_metadata:
        clean_metadata.update(extra_metadata)

    # Auto-build data dictionary if not explicitly provided
    if "dagster/column_schema" not in clean_metadata:
        if is_3tuple:
            clean_metadata["dagster/column_schema"] = build_table_schema(
                schema, derived_columns=derived_columns,
            )
        else:
            clean_metadata["dagster/column_schema"] = build_table_schema_from_contract(
                schema_contract, derived_columns=derived_columns,
            )

    clean_tags = _build_standard_tags(
        stage="clean",
        source="checkbook",
        domain=domain,
        geographic_scope=geographic_scope,
        extra_tags=tags,
    )

    clean_spec = AssetSpec(
        key=name,
        description=description or f"Cleaned version of {name}",
        group_name=effective_group,
        kinds={"polars", "clean"},
        partitions_def=final_clean_partitions,
        deps=clean_deps,
        metadata=clean_metadata,
        tags=clean_tags,
    )

    combined_post = post_transform_fn
    if enrichment_transform and post_transform_fn:
        _enrich, _custom = enrichment_transform, post_transform_fn

        def combined_post(lf: pl.LazyFrame) -> pl.LazyFrame:
            return _custom(_enrich(lf))
    elif enrichment_transform:
        combined_post = enrichment_transform

    def _pipeline_logic(lf: pl.LazyFrame) -> pl.LazyFrame:
        lf = apply_schema_contract(lf, schema_contract)
        if combined_post:
            lf = combined_post(lf)
        return lf

    clean_asset = define_clean_asset(
        clean_spec,
        transform_fn=_pipeline_logic,
        schema_contract=schema_contract,
        post_transform_fn=combined_post,
        diagnostics=diagnostics,
    )

    # --- 3. Row Accounting Check ---
    grain = _infer_partition_grain(partitions_def, final_clean_partitions)
    checks: list[AssetsDefinition] = [
        define_row_accounting_check(
            clean_key=name,
            landing_key=landing_key,
            partition_grain=grain,
            allows_row_drop=expect_filtered_rows,
            clean_io_manager_key="clean_large_io_manager",
            partitions_def=final_clean_partitions,
        ),
    ]

    # --- 4. Cross-Partition Summary (partitioned assets only) ---
    if final_clean_partitions:
        checks.append(define_cross_partition_summary_check(
            clean_key=name,
            landing_key=landing_key,
            partition_grain=grain,
            clean_io_manager_key="clean_large_io_manager",
        ))

    return PipelineResult(
        landing=landing_asset,
        clean=clean_asset,
        checks=checks,
    )
