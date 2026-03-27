from opendata_framework.dagster import SocrataIngestConfig
from opendata_framework.dagster.factories import create_socrata_pipeline

from opendata_eda.defs.assets.floodnet._shared import (
    SENSOR_METADATA_SCHEMA,
    SENSOR_METADATA_METADATA,
)

sensor_metadata_pipeline = create_socrata_pipeline(
    name="nyc_floodnet_sensor_metadata",
    domain="environment",
    geographic_scope="nyc",
    url="https://data.cityofnewyork.us/Environment/FloodNet-Sensor-Deployment-Metadata/kb2e-tjy3",
    owner="NYC OpenData",
    description="FloodNet sensor deployment metadata. Location, installation dates, and geographic info.",
    socrata_config=SocrataIngestConfig(
        endpoint="kb2e-tjy3",
        time_col="date_installed",
        order_field="date_installed DESC",
        limit=50_000,
        base_domain="data.cityofnewyork.us",
    ),
    schema=SENSOR_METADATA_SCHEMA,
    extra_metadata=SENSOR_METADATA_METADATA,
)

nyc_floodnet_sensor_metadata_landing = sensor_metadata_pipeline.landing
nyc_floodnet_sensor_metadata = sensor_metadata_pipeline.clean
