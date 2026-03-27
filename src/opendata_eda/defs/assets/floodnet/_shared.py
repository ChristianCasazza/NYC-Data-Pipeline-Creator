import polars as pl
from dagster import MetadataValue, TableColumn, TableSchema

from opendata_framework.dagster import SchemaContract
from opendata_framework.core import build_table_schema

FLOODING_EVENTS_SCHEMA: SchemaContract = {
    "sensor_name":                    ("sensor_name", pl.Utf8, "The name used to describe the sensor."),
    "sensor_id":                      ("sensor_id", pl.Utf8, "The unique ID used to identify the sensor location."),
    "flood_start_time":               ("flood_start_time", pl.Datetime, "Start time of the measured flood event."),
    "flood_end_time":                 ("flood_end_time", pl.Datetime, "End time of the measured flood event."),
    "max_depth_inches":               ("max_depth_inches", pl.Float64, "Maximum flood depth (inches) during the event."),
    "onset_time_mins":                ("onset_time_mins", pl.Float64, "Minutes after flood start to reach max depth."),
    "drain_time_mins":                ("drain_time_mins", pl.Float64, "Minutes to drain from max depth to 0 inches."),
    "duration_mins":                  ("duration_mins", pl.Float64, "Duration of the flood event in minutes."),
    "duration_above_4_inches_mins":   ("duration_above_4_inches_mins", pl.Float64, "Minutes water depth was >= 4 inches."),
    "duration_above_12_inches_mins":  ("duration_above_12_inches_mins", pl.Float64, "Minutes water depth was >= 12 inches."),
    "duration_above_24_inches_mins":  ("duration_above_24_inches_mins", pl.Float64, "Minutes water depth was >= 24 inches."),
    "flood_profile_depth_inches":     ("flood_profile_depth_inches", pl.Utf8, "JSON array of flood depths with timestamps."),
    "flood_profile_time_secs":        ("flood_profile_time_secs", pl.Utf8, "JSON array of relative times (seconds from start)."),
}

SENSOR_METADATA_SCHEMA: SchemaContract = {
    "sensor_name":                         ("sensor_name", pl.Utf8, "The name used to describe the sensor."),
    "sensor_id":                           ("sensor_id", pl.Utf8, "The unique ID used to identify the sensor location."),
    "date_installed":                      ("date_installed", pl.Date, "Date the sensor was installed."),
    "tidally_influenced":                  ("tidally_influenced", pl.Utf8, "Whether sensor is affected by tides."),
    "date_removed":                        ("date_removed", pl.Date, "Date the sensor was removed, if applicable."),
    "street_name":                         ("street_name", pl.Utf8, "Street the sensor is installed on."),
    "borough":                             ("borough", pl.Utf8, "NYC borough."),
    "zipcode":                             ("zipcode", pl.Utf8, "Zip code."),
    "community_board":                     ("community_board", pl.Int64, "Community board number."),
    "council_district":                    ("council_district", pl.Utf8, "Council district."),
    "census_tract":                        ("census_tract", pl.Utf8, "2020 census tract."),
    "nta":                                 ("nta", pl.Utf8, "Neighborhood Tabulation Area."),
    "latitude":                            ("latitude", pl.Float64, "Latitude."),
    "longitude":                           ("longitude", pl.Float64, "Longitude."),
    "lowest_point_height_delta_inches":    ("lowest_point_height_delta_inches", pl.Float64, "Elevation delta to lowest local point (inches)."),
    "location":                            ("location", pl.Utf8, "WKT point location (redundant with lat/lon)."),
}

FLOODING_EVENTS_METADATA = {
    "source_portal_url": MetadataValue.url("https://data.cityofnewyork.us/Environment/FloodNet-Street-Flooding-Events-Measured-by-FloodN/aq7i-eu5q"),
    "data_owner": MetadataValue.text("NYC OpenData"),
    "dagster/column_schema": build_table_schema(FLOODING_EVENTS_SCHEMA),
}

SENSOR_METADATA_METADATA = {
    "source_portal_url": MetadataValue.url("https://data.cityofnewyork.us/Environment/FloodNet-Sensor-Deployment-Metadata/kb2e-tjy3"),
    "data_owner": MetadataValue.text("NYC OpenData"),
    "dagster/column_schema": build_table_schema(SENSOR_METADATA_SCHEMA),
}

JOINED_DERIVED_COLS = [
    TableColumn(name="has_imputed_timestamp", type="Boolean", description="True if flood_start_time or flood_end_time was reconstructed from duration_mins."),
    TableColumn(name="is_tidally_influenced", type="Boolean", description="Boolean cast of tidally_influenced (Yes=true, No=false)."),
    TableColumn(name="flood_severity", type="Utf8", description="Categorical severity: minor (<4in), moderate (4-12in), major (12-24in), severe (>=24in)."),
    TableColumn(name="flood_year", type="Int32", description="Year from flood_start_time."),
    TableColumn(name="flood_month", type="Int32", description="Month (1-12) from flood_start_time."),
    TableColumn(name="flood_hour", type="Int32", description="Hour (0-23) from flood_start_time."),
    TableColumn(name="flood_season", type="Utf8", description="Season: winter, spring, summer, fall."),
    TableColumn(name="is_overnight", type="Boolean", description="True if flood started between 10 PM and 5 AM."),
    TableColumn(name="rise_rate_in_per_min", type="Float64", description="max_depth / onset_time (in/min)."),
    TableColumn(name="drain_rate_in_per_min", type="Float64", description="max_depth / drain_time (in/min)."),
    TableColumn(name="drain_efficiency_ratio", type="Float64", description="drain_time / onset_time. >1 = slower drain than rise."),
    TableColumn(name="time_to_peak_pct", type="Float64", description="onset_time / duration. Lower = flashier flood."),
    TableColumn(name="flood_volume_index", type="Float64", description="Trapezoidal area under depth-time curve (inch-minutes)."),
    TableColumn(name="profile_sample_count", type="Int32", description="Number of data points in flood profile."),
]

METADATA_ENRICH_COLS = [
    "sensor_id", "date_installed", "tidally_influenced", "date_removed",
    "street_name", "borough", "zipcode", "community_board",
    "council_district", "census_tract", "nta", "latitude", "longitude",
    "lowest_point_height_delta_inches",
]


def build_joined_table_schema() -> TableSchema:
    events_schema = build_table_schema(FLOODING_EVENTS_SCHEMA)
    metadata_cols = [
        TableColumn(name=dst, type=str(dtype), description=desc)
        for _, (dst, dtype, desc) in SENSOR_METADATA_SCHEMA.items()
        if dst not in ("sensor_name", "sensor_id", "location")
    ]
    return TableSchema(columns=list(events_schema.columns) + metadata_cols + JOINED_DERIVED_COLS)
