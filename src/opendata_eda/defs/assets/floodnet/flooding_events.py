from opendata_framework.dagster import SocrataIngestConfig
from opendata_framework.dagster.factories import create_socrata_pipeline

from opendata_eda.defs.assets.floodnet._shared import (
    FLOODING_EVENTS_SCHEMA,
    FLOODING_EVENTS_METADATA,
)

flooding_events_pipeline = create_socrata_pipeline(
    name="nyc_floodnet_flooding_events",
    domain="environment",
    geographic_scope="nyc",
    url="https://data.cityofnewyork.us/Environment/FloodNet-Street-Flooding-Events-Measured-by-FloodN/aq7i-eu5q",
    owner="NYC OpenData",
    description="FloodNet street flooding events. Depth measurements analyzed into event summaries.",
    socrata_config=SocrataIngestConfig(
        endpoint="aq7i-eu5q",
        time_col="flood_start_time",
        order_field="flood_start_time DESC",
        limit=50_000,
        base_domain="data.cityofnewyork.us",
    ),
    schema=FLOODING_EVENTS_SCHEMA,
    extra_metadata=FLOODING_EVENTS_METADATA,
)

nyc_floodnet_flooding_events_landing = flooding_events_pipeline.landing
nyc_floodnet_flooding_events = flooding_events_pipeline.clean
