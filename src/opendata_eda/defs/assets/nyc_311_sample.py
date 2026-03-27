import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
    monthly_partitions,
    yearly_partitions,
)
from opendata_framework.enrichments import StandardEnrichments, TemporalConfig, TimestampConfig

nyc_311_schema: SchemaContract = {
    "unique_key": ("unique_key", pl.Utf8, "Unique service request ID"),
    "created_date": ("created_date", pl.Datetime, "Date request was filed"),
    "agency_name": ("agency_name", pl.Utf8, "Responding city agency"),
    "complaint_type": ("complaint_type", pl.Utf8, "Type of complaint"),
    "borough": ("borough", pl.Utf8, "NYC borough"),
}

nyc_311_pipeline = create_socrata_pipeline(
    name="nyc_311_sample",
    domain="operations",
    geographic_scope="nyc",
    socrata_config=SocrataIngestConfig(
        endpoint="erm2-nwe9",
        time_col="created_date",
        base_domain="data.cityofnewyork.us",
    ),
    schema=nyc_311_schema,
    description="Sample 311 pipeline — monthly landing, yearly clean",
    partitions_def=monthly_partitions("2026-01-01", end_offset=1),
    clean_partitions_def=yearly_partitions("2026", end_offset=1),
    enrichments=StandardEnrichments(
        timestamp=TimestampConfig(source_col="created_date", precision="day"),
        temporal=TemporalConfig(year=True, month=True),
    ),
)

nyc_311_sample = nyc_311_pipeline.clean
