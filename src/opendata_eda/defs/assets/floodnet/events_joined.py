import json

import polars as pl
from dagster import AssetKey, AutomationCondition, MaterializeResult, MetadataValue, asset

from opendata_framework.enrichments import (
    add_completeness_flags,
    add_record_timestamp,
    add_temporal_columns,
    compute_rate,
)

from opendata_eda.defs.assets.floodnet._shared import (
    METADATA_ENRICH_COLS,
    build_joined_table_schema,
)


def _compute_flood_volume_index(depth_str: str, time_str: str) -> float | None:
    """Trapezoidal area under depth-vs-time curve (inch-minutes)."""
    try:
        depths = json.loads(depth_str)
        times_secs = json.loads(time_str)
    except (json.JSONDecodeError, TypeError):
        return None
    if len(depths) < 2 or len(depths) != len(times_secs):
        return None
    area = 0.0
    for i in range(1, len(depths)):
        dt_min = (times_secs[i] - times_secs[i - 1]) / 60.0
        area += (depths[i - 1] + depths[i]) / 2.0 * dt_min
    return round(area, 2)


def _profile_sample_count(depth_str: str) -> int | None:
    try:
        return len(json.loads(depth_str))
    except (json.JSONDecodeError, TypeError):
        return None


@asset(
    key="nyc_floodnet_events_joined",
    group_name="nyc__environment",
    kinds={"polars", "clean", "joined"},
    deps=[
        AssetKey("nyc_floodnet_flooding_events"),
        AssetKey("nyc_floodnet_sensor_metadata"),
    ],
    io_manager_key="clean_single_io_manager",
    required_resource_keys={"clean_large_io_manager"},
    automation_condition=AutomationCondition.eager(),
    tags={"domain": "environment", "geographic_scope": "nyc", "stage": "clean", "type": "joined"},
    metadata={
        "dagster/column_schema": build_joined_table_schema(),
        "source_portal_url": MetadataValue.url("https://data.cityofnewyork.us/Environment/FloodNet-Street-Flooding-Events-Measured-by-FloodN/aq7i-eu5q"),
        "data_owner": MetadataValue.text("NYC OpenData"),
    },
    description="Flooding events enriched with sensor metadata, imputed timestamps, severity "
                "classification, hydro metrics, and flood profile summaries. One row per event.",
)
def nyc_floodnet_events_joined(context) -> MaterializeResult:
    io = context.resources.clean_large_io_manager

    events_path = io.get_path_for_asset(AssetKey("nyc_floodnet_flooding_events"))
    metadata_path = io.get_path_for_asset(AssetKey("nyc_floodnet_sensor_metadata"))

    lf_events = pl.scan_parquet(str(events_path))
    lf_metadata = pl.scan_parquet(str(metadata_path)).select(METADATA_ENRICH_COLS)

    lf_joined = lf_events.join(lf_metadata, on="sensor_id", how="left")
    df = lf_joined.collect()

    # 1. Impute null timestamps
    duration_td = pl.duration(microseconds=pl.col("duration_mins") * 60 * 1_000_000)

    df = df.with_columns(
        (pl.col("flood_start_time").is_null() | pl.col("flood_end_time").is_null())
        .alias("has_imputed_timestamp"),
    )
    df = df.with_columns(
        pl.col("flood_start_time")
        .fill_null(pl.col("flood_end_time") - duration_td)
        .alias("flood_start_time"),
    )
    df = df.with_columns(
        pl.col("flood_end_time")
        .fill_null(pl.col("flood_start_time") + duration_td)
        .alias("flood_end_time"),
    )

    # Cast DATE columns to TIMESTAMP for DuckDB-WASM compatibility
    df = df.with_columns(
        pl.col("date_installed").cast(pl.Datetime("us", "America/New_York")),
        pl.col("date_removed").cast(pl.Datetime("us", "America/New_York")),
    )

    # 2. Derived columns
    severity = (
        pl.when(pl.col("max_depth_inches") < 4.0).then(pl.lit("minor"))
        .when(pl.col("max_depth_inches") < 12.0).then(pl.lit("moderate"))
        .when(pl.col("max_depth_inches") < 24.0).then(pl.lit("major"))
        .otherwise(pl.lit("severe"))
        .alias("flood_severity")
    )

    df = df.with_columns(
        (pl.col("tidally_influenced") == "Yes").alias("is_tidally_influenced"),
        severity,
        compute_rate("max_depth_inches", "onset_time_mins", "rise_rate_in_per_min", round_digits=4),
        compute_rate("max_depth_inches", "drain_time_mins", "drain_rate_in_per_min", round_digits=4),
        compute_rate("drain_time_mins", "onset_time_mins", "drain_efficiency_ratio", round_digits=4),
        compute_rate("onset_time_mins", "duration_mins", "time_to_peak_pct", round_digits=4),
    )

    # Temporal columns
    df = add_temporal_columns(
        df.lazy(), "flood_start_time",
        prefix="flood_",
        year=True, month=True, hour=True, season=True, is_overnight=True,
        quarter=False, day_of_week=False, fiscal_year=False, year_month_key=False,
    ).collect()

    df = add_record_timestamp(df.lazy(), pl.col("flood_start_time"), "datetime").collect()
    df = add_completeness_flags(df.lazy(), lat_col="latitude", lon_col="longitude").collect()

    # 3. Profile-derived columns
    df = df.with_columns(
        pl.struct("flood_profile_depth_inches", "flood_profile_time_secs")
        .map_elements(
            lambda row: _compute_flood_volume_index(
                row["flood_profile_depth_inches"], row["flood_profile_time_secs"]
            ),
            return_dtype=pl.Float64,
        )
        .alias("flood_volume_index"),
        pl.col("flood_profile_depth_inches")
        .map_elements(_profile_sample_count, return_dtype=pl.Int32)
        .alias("profile_sample_count"),
    )

    df = df.sort("flood_start_time", "sensor_id")

    return MaterializeResult(value=df)
