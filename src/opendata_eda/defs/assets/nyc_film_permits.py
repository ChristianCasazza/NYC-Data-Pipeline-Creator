import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

film_permits_schema: SchemaContract = {
    "eventid": ("event_id", pl.Utf8, "Unique permit event ID"),
    "eventtype": ("event_type", pl.Utf8, "Type of event (shooting, theater, etc.)"),
    "startdatetime": ("start_date", pl.Utf8, "Permit start date/time"),
    "enddatetime": ("end_date", pl.Utf8, "Permit end date/time"),
    "borough": ("borough", pl.Utf8, "NYC borough"),
    "category": ("category", pl.Utf8, "Production category"),
}

film_permits_pipeline = create_socrata_pipeline(
    name="nyc_film_permits",
    socrata_config=SocrataIngestConfig(
        endpoint="tg4x-b46p",
        time_col="startdatetime",
        base_domain="data.cityofnewyork.us",
    ),
    schema=film_permits_schema,
    description="NYC film permits — full snapshot, no partitions",
)

nyc_film_permits = film_permits_pipeline.clean
