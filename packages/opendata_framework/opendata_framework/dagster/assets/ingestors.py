# opendata_framework/dagster/assets/ingestors.py
import inspect
import os
import tempfile
import textwrap
from collections.abc import Callable
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import requests
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetSpec,
    AssetsDefinition,
    AutomationCondition,
    Backoff,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    multi_asset,
)

from opendata_framework.core.polars_utils import SchemaContract, apply_schema_contract
from opendata_framework.dagster.standards import CheckbookIngestConfig, HttpIngestConfig, SocrataIngestConfig

if TYPE_CHECKING:
    from opendata_framework.dagster.factories import CleanDiagnosticsConfig

def _extract_function_source(fn: Callable | None) -> str:
    """Safely extracts source code."""
    if fn is None: return "No transformation function provided."
    try:
        target_fn = fn.func if isinstance(fn, partial) else fn
        return textwrap.dedent(inspect.getsource(target_fn))
    except Exception as e:
        return f"Unable to inspect source code: {str(e)}"

def _generate_schema_markdown(schema: dict) -> str:
    """Converts a Polars schema dict to a Markdown table."""
    md_lines = ["| Column Name | Polars Type |", "| :--- | :--- |"]
    for col_name, dtype in schema.items():
        dtype_str = str(dtype).replace("<", "&lt;").replace(">", "&gt;")
        md_lines.append(f"| **{col_name}** | `{dtype_str}` |")
    return "\n".join(md_lines)

def define_socrata_landing_source(spec: AssetSpec) -> AssetsDefinition:
    """
    Stage 1: Ingests raw CSV pages from Socrata to the Landing layer.
    """
    updated_metadata = {**spec.metadata, "lazy": False}
    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["socrata", "landing", "csv"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        metadata=updated_metadata,
        tags=tags,
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"ingestor_socrata_landing_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        required_resource_keys={"socrata"},
        retry_policy=RetryPolicy(max_retries=3, delay=60, backoff=Backoff.EXPONENTIAL)
    )
    def _ingest_socrata_landing(context: AssetExecutionContext) -> MaterializeResult:
        asset_key = context.asset_key
        spec_meta = context.assets_def.specs_by_key[asset_key].metadata
        config = SocrataIngestConfig(**spec_meta["socrata_config"])
        socrata = context.resources.socrata
        
        where_clause = "1=1"
        if context.has_partition_key:
            if config.partition_filter_type == "time":
                time_window = context.partition_time_window
                start_str = time_window.start.strftime("%Y-%m-%dT%H:%M:%S")
                end_str = time_window.end.strftime("%Y-%m-%dT%H:%M:%S")
                where_clause = f"{config.partition_col} >= '{start_str}' AND {config.partition_col} < '{end_str}'"
            elif config.partition_filter_type == "equality":
                where_clause = f"{config.partition_col} = '{context.partition_key}'"
        
        # Get the Page Generator
        # We iterate over pages, yielding (batch_index, stream)
        page_gen = socrata.get_csv_page_generator(
            endpoint=config.endpoint,
            where_clause=where_clause,
            base_domain=config.base_domain,
            limit=config.limit,
            order_field=config.order_field or ":id"
        )
        
        return MaterializeResult(
            asset_key=asset_key,
            value=page_gen, # Passed to LandingIOManager.handle_output
            metadata={
                "filter": where_clause, 
                "format": "sharded_csv"
            }
        )

    return _ingest_socrata_landing

def define_raw_from_landing_source(spec: AssetSpec) -> AssetsDefinition:
    """
    Stage 2: Converts Landing CSV shards to Raw Parquet.
    """
    deps = list(spec.deps)
    upstream_key = deps[0].asset_key
    input_arg_name = upstream_key.path[-1].replace("-", "_").replace(".", "_")

    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["polars", "raw", "parquet"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        tags=tags,
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"loader_raw_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        ins={input_arg_name: AssetIn(key=upstream_key)},
        required_resource_keys={"landing_io_manager"}
    )
    def _load_raw_from_landing(context: AssetExecutionContext, **inputs) -> MaterializeResult:
        # Input is a Polars DataFrame (read from multiple CSV shards by IO Manager)
        df = inputs[input_arg_name]
        
        if df.is_empty():
            return MaterializeResult(
                asset_key=spec.key,
                value=pl.DataFrame([]),
                metadata={"record_count": 0}
            )

        return MaterializeResult(
            asset_key=spec.key,
            value=df,
            metadata={
                "record_count": df.height,
                "columns": len(df.columns)
            }
        )
        
    return _load_raw_from_landing

def define_checkbook_landing_source(spec: AssetSpec) -> AssetsDefinition:
    """
    Stage 1: Ingests spending data from the Checkbook NYC XML API to the Landing layer.

    Uses issue_date range filtering from monthly partition time windows.
    Yields (batch_index, csv_bytes_iterator) tuples via paginated XML POST requests.
    """
    updated_metadata = {**spec.metadata, "lazy": False}
    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["checkbook", "landing", "csv"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        metadata=updated_metadata,
        tags=tags,
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"ingestor_checkbook_landing_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        required_resource_keys={"checkbook_api"},
        retry_policy=RetryPolicy(max_retries=3, delay=60, backoff=Backoff.EXPONENTIAL),
        pool="checkbook_api",
    )
    def _ingest_checkbook_landing(context: AssetExecutionContext) -> MaterializeResult:
        asset_key = context.asset_key
        spec_meta = context.assets_def.specs_by_key[asset_key].metadata
        config = CheckbookIngestConfig(**spec_meta["checkbook_config"])
        checkbook = context.resources.checkbook_api

        # Build criteria based on filter strategy
        if config.filter_type == "date_range":
            time_window = context.partition_time_window
            start_str = time_window.start.strftime("%Y-%m-%d")
            end_str = time_window.end.strftime("%Y-%m-%d")
            criteria = [
                {
                    "name": config.filter_field,
                    "type": "range",
                    "start": start_str,
                    "end": end_str,
                },
            ]
            filter_desc = f"{start_str} to {end_str}"
        elif config.filter_type == "fiscal_year":
            year_key = context.partition_key
            criteria = [
                {
                    "name": config.filter_field,
                    "type": "value",
                    "value": year_key,
                },
            ]
            filter_desc = f"fiscal_year={year_key}"
        else:
            msg = f"Unknown filter_type: {config.filter_type}"
            raise ValueError(msg)

        criteria.extend(config.extra_criteria)

        page_gen = checkbook.get_page_generator(
            type_of_data=config.type_of_data,
            criteria=criteria,
            response_columns=config.response_columns,
        )

        return MaterializeResult(
            asset_key=asset_key,
            value=page_gen,
            metadata={
                "filter": filter_desc,
                "format": "sharded_csv",
            },
        )

    return _ingest_checkbook_landing


def define_http_source(spec: AssetSpec) -> AssetsDefinition:
    """Creates an asset that downloads a file from a static HTTP URL."""
    updated_metadata = {**spec.metadata, "lazy": True}
    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["http", "file"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        metadata=updated_metadata,
        tags=tags,
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"ingestor_http_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        retry_policy=RetryPolicy(max_retries=3, delay=30)
    )
    def _ingest_http(context: AssetExecutionContext) -> MaterializeResult:
        asset_key = context.asset_key
        spec_meta = context.assets_def.specs_by_key[asset_key].metadata
        config = HttpIngestConfig(**spec_meta["http_config"])

        with tempfile.NamedTemporaryFile(delete=True) as tf:
            with requests.get(config.url, stream=True, headers={"User-Agent": config.user_agent}) as r:
                r.raise_for_status()
                for chunk in r.iter_content(chunk_size=1024*1024):
                    tf.write(chunk)
                tf.flush()
                
            if config.format == "parquet":
                df = pl.read_parquet(tf.name)
            elif config.format == "csv":
                df = pl.read_csv(tf.name, ignore_errors=True)
            elif config.format == "json":
                df = pl.read_json(tf.name)
            else:
                raise ValueError(f"Unsupported format: {config.format}")

        return MaterializeResult(
            asset_key=asset_key,
            value=df,
            metadata={"row_count": df.height, "source_url": config.url}
        )

    return _ingest_http

def define_http_parquet_download(spec: AssetSpec) -> AssetsDefinition:
    """Creates an asset that streams a parquet file from HTTP directly to disk.

    Unlike define_http_source, this does NOT read the file into a DataFrame.
    The file is written directly to the IO manager's expected path so
    downstream assets can load it via the IO manager as a LazyFrame.
    """
    io_manager_key = spec.metadata.get("dagster/io_manager_key", "raw_single_io_manager")

    updated_metadata = {**spec.metadata, "lazy": True}
    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["http", "parquet"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        metadata=updated_metadata,
        tags=tags,
        automation_condition=spec.automation_condition or AutomationCondition.missing(),
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"download_parquet_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        required_resource_keys={io_manager_key},
        retry_policy=RetryPolicy(max_retries=3, delay=30),
    )
    def _download_parquet(context: AssetExecutionContext) -> MaterializeResult:
        asset_key = context.asset_key
        spec_meta = context.assets_def.specs_by_key[asset_key].metadata
        config = HttpIngestConfig(**spec_meta["http_config"])

        io_mgr = getattr(context.resources, io_manager_key)
        target_path = io_mgr.get_path_for_asset(asset_key)
        target_path.parent.mkdir(parents=True, exist_ok=True)

        context.log.info(f"Streaming {config.url} -> {target_path}")

        with requests.get(config.url, stream=True, headers={"User-Agent": config.user_agent}) as r:
            r.raise_for_status()
            bytes_written = 0
            with target_path.open("wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    bytes_written += len(chunk)

        size_mb = bytes_written / (1024 * 1024)
        context.log.info(f"Downloaded {size_mb:.1f} MB to {target_path}")

        return MaterializeResult(
            asset_key=asset_key,
            metadata={
                "path": MetadataValue.path(str(target_path)),
                "source_url": config.url,
                "size_mb": round(size_mb, 2),
            },
        )

    return _download_parquet


def _build_dropped_rows_md(dropped: "pl.DataFrame", max_rows: int = 20) -> str:
    """Build a markdown table showing a sample of dropped rows."""
    sample = dropped.head(max_rows)
    cols = sample.columns
    visible_cols = [c for c in cols if sample[c].null_count() < sample.height]
    if not visible_cols:
        return ""
    md_lines = [
        f"**{dropped.height} rows dropped by transform** (showing first {min(sample.height, dropped.height)}):",
        "",
        "| " + " | ".join(visible_cols) + " |",
        "| " + " | ".join(["---"] * len(visible_cols)) + " |",
    ]
    for row in sample.iter_rows(named=True):
        vals = [str(row[c]) if row[c] is not None else "" for c in visible_cols]
        vals = [v[:60] + "..." if len(v) > 60 else v for v in vals]
        md_lines.append("| " + " | ".join(vals) + " |")
    return "\n".join(md_lines)


def _compute_dropped_row_metadata(
    *,
    context: AssetExecutionContext,
    spec: AssetSpec,
    raw_lf: pl.LazyFrame,
    schema_contract,
    post_transform_fn,
    mode: str,
    csv_threshold: int,
) -> dict[str, Any]:
    """Compute dropped-row metadata for a clean transform when explicitly enabled."""
    if schema_contract is None or post_transform_fn is None:
        return {}

    try:
        indexed = raw_lf.with_row_index("__row_idx")
        schema_applied = apply_schema_contract(indexed, schema_contract, drop_unknown=False)
        enriched = post_transform_fn(schema_applied).collect()

        if "__row_idx" not in enriched.columns:
            return {}

        survived_idx = set(enriched["__row_idx"].to_list())
        all_rows = schema_applied.collect()
        dropped = all_rows.filter(
            ~pl.col("__row_idx").is_in(survived_idx)
        ).drop("__row_idx")

        if dropped.height == 0:
            return {}

        sample = dropped.head(20)
        sample_records = [
            {col: (str(val) if val is not None else None) for col, val in zip(sample.columns, row)}
            for row in sample.iter_rows()
        ]
        dropped_metadata: dict[str, Any] = {
            "dropped_rows_sample": MetadataValue.json(sample_records),
            "dropped_rows_count": dropped.height,
        }

        if mode == "full" or dropped.height > csv_threshold:
            data_root = Path(os.environ.get("DATA_ROOT", "./data"))
            asset_name = spec.key.path[-1]
            diag_dir = data_root / "opendata" / "diagnostics" / asset_name
            diag_dir.mkdir(parents=True, exist_ok=True)

            op_ctx = context.op_execution_context
            partition_key = op_ctx.partition_key if op_ctx.has_partition_key else None
            suffix = f"_{partition_key}" if partition_key else ""
            csv_path = str(diag_dir / f"dropped_rows{suffix}.csv")
            dropped.write_csv(csv_path)
            dropped_metadata["dropped_rows_csv"] = MetadataValue.path(csv_path)

        return dropped_metadata
    except Exception as e:
        return {"dropped_rows_error": MetadataValue.text(f"Could not compute: {e}")}


def define_clean_asset(
    spec: AssetSpec,
    transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    schema_contract: SchemaContract | None = None,
    post_transform_fn: Callable[[pl.LazyFrame], pl.LazyFrame] | None = None,
    diagnostics: "CleanDiagnosticsConfig | None" = None,
) -> AssetsDefinition:
    """Creates a transformation asset that processes an upstream dependency.

    Parameters
    ----------
    transform_fn:
        Combined transform (schema contract + enrichment). Always required.
    schema_contract:
        Optional schema contract dict. When provided with post_transform_fn,
        enables dropped-row analysis by running the two phases separately.
    post_transform_fn:
        Optional enrichment function. Used for dropped-row analysis only.
    diagnostics:
        Optional diagnostics config from the factory layer. When omitted, the
        clean stage stays lazy by default and relies on asset checks for row
        accounting.
    """
    deps = list(spec.deps)
    upstream_key = deps[0].asset_key
    input_arg_name = upstream_key.path[-1].replace("-", "_").replace(".", "_")
    
    tags = dict(spec.tags) if spec.tags else {}
    for kind in ["polars", "clean"]:
        tags[f"dagster/kind/{kind}"] = ""

    updated_spec = spec._replace(
        tags=tags,
    )

    @multi_asset(
        specs=[updated_spec],
        name=f"cleaner_{spec.key.to_user_string().replace('/', '_').replace('.', '_')}",
        ins={input_arg_name: AssetIn(key=upstream_key)},
        required_resource_keys={"landing_io_manager"}
    )
    def _clean_polars(context: AssetExecutionContext, **inputs) -> MaterializeResult:
        lf = inputs[input_arg_name]
        diag = diagnostics
        code_artifact = (
            _extract_function_source(transform_fn)
            if diag is None or getattr(diag, "capture_transform_source", True)
            else "Transform source capture disabled."
        )

        if transform_fn:
            lf = transform_fn(lf)

        metadata: dict[str, Any] = {
            "dagster/code_version": context.run_id,
        }

        if diag is None or getattr(diag, "capture_transform_source", True):
            metadata["transformation_logic"] = MetadataValue.md(f"```python\n{code_artifact}\n```")

        if diag is None or getattr(diag, "capture_output_schema", True):
            try:
                final_schema = lf.collect_schema()
                schema_artifact = _generate_schema_markdown(final_schema)
            except Exception as e:
                schema_artifact = f"Could not resolve schema lazily: {str(e)}"
            metadata["output_schema"] = MetadataValue.md(schema_artifact)

        if diag is not None and getattr(diag, "capture_row_counts", False):
            pre_transform_rows = -1
            post_transform_rows = -1
            try:
                pre_transform_rows = inputs[input_arg_name].select(pl.len()).collect().item()
            except Exception as e:
                context.log.warning("Could not count pre-transform rows: %s", e)
            try:
                post_transform_rows = lf.select(pl.len()).collect().item()
            except Exception as e:
                context.log.warning("Could not count post-transform rows: %s", e)

            transform_row_delta = 0
            if pre_transform_rows >= 0 and post_transform_rows >= 0:
                transform_row_delta = pre_transform_rows - post_transform_rows

            metadata.update({
                "pre_transform_rows": pre_transform_rows,
                "post_transform_rows": post_transform_rows,
                "transform_row_delta": transform_row_delta,
            })

        if diag is not None and getattr(diag, "capture_dropped_rows", "off") != "off":
            metadata.update(_compute_dropped_row_metadata(
                context=context,
                spec=spec,
                raw_lf=inputs[input_arg_name],
                schema_contract=schema_contract,
                post_transform_fn=post_transform_fn,
                mode=getattr(diag, "capture_dropped_rows", "off"),
                csv_threshold=getattr(diag, "dropped_rows_csv_threshold", 20),
            ))

        return MaterializeResult(
            asset_key=spec.key,
            value=lf,
            metadata=metadata,
        )

    return _clean_polars
