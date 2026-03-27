import polars as pl

from opendata_framework.dagster import (
    create_socrata_pipeline,
    SocrataIngestConfig,
    SchemaContract,
)

collisions_schema: SchemaContract = {
    "crash_date":                      ("crash_date", pl.Date, "Occurrence date of collision."),
    "crash_time":                      ("crash_time", pl.Utf8, "Occurrence time of collision (HH:MM)."),
    "borough":                         ("borough", pl.Utf8, "Borough where collision occurred."),
    "zip_code":                        ("zip_code", pl.Utf8, "Postal code of incident occurrence."),
    "latitude":                        ("latitude", pl.Float64, "Latitude (WGS 84, EPSG 4326)."),
    "longitude":                       ("longitude", pl.Float64, "Longitude (WGS 84, EPSG 4326)."),
    "location":                        ("location", pl.Utf8, "Latitude/longitude pair as string."),
    "on_street_name":                  ("on_street_name", pl.Utf8, "Street on which the collision occurred."),
    "off_street_name":                 ("off_street_name", pl.Utf8, "Nearest cross street to the collision."),
    "cross_street_name":               ("cross_street_name", pl.Utf8, "Street address if known."),
    "number_of_persons_injured":       ("number_of_persons_injured", pl.Float64, "Total persons injured."),
    "number_of_persons_killed":        ("number_of_persons_killed", pl.Float64, "Total persons killed."),
    "number_of_pedestrians_injured":   ("number_of_pedestrians_injured", pl.Float64, "Pedestrians injured."),
    "number_of_pedestrians_killed":    ("number_of_pedestrians_killed", pl.Float64, "Pedestrians killed."),
    "number_of_cyclist_injured":       ("number_of_cyclist_injured", pl.Float64, "Cyclists injured."),
    "number_of_cyclist_killed":        ("number_of_cyclist_killed", pl.Float64, "Cyclists killed."),
    "number_of_motorist_injured":      ("number_of_motorist_injured", pl.Float64, "Vehicle occupants injured."),
    "number_of_motorist_killed":       ("number_of_motorist_killed", pl.Float64, "Vehicle occupants killed."),
    "contributing_factor_vehicle_1":   ("contributing_factor_vehicle_1", pl.Utf8, "Contributing factor for vehicle 1."),
    "contributing_factor_vehicle_2":   ("contributing_factor_vehicle_2", pl.Utf8, "Contributing factor for vehicle 2."),
    "contributing_factor_vehicle_3":   ("contributing_factor_vehicle_3", pl.Utf8, "Contributing factor for vehicle 3."),
    "contributing_factor_vehicle_4":   ("contributing_factor_vehicle_4", pl.Utf8, "Contributing factor for vehicle 4."),
    "contributing_factor_vehicle_5":   ("contributing_factor_vehicle_5", pl.Utf8, "Contributing factor for vehicle 5."),
    "collision_id":                    ("collision_id", pl.Int64, "Unique record code (primary key)."),
    "vehicle_type_code1":              ("vehicle_type_code_1", pl.Utf8, "Vehicle 1 type (car/suv, truck/bus, bicycle, etc.)."),
    "vehicle_type_code2":              ("vehicle_type_code_2", pl.Utf8, "Vehicle 2 type."),
    "vehicle_type_code_3":             ("vehicle_type_code_3", pl.Utf8, "Vehicle 3 type."),
    "vehicle_type_code_4":             ("vehicle_type_code_4", pl.Utf8, "Vehicle 4 type."),
    "vehicle_type_code_5":             ("vehicle_type_code_5", pl.Utf8, "Vehicle 5 type."),
}

collisions_pipeline = create_socrata_pipeline(
    name="nyc_motor_vehicle_collisions",
    socrata_config=SocrataIngestConfig(
        endpoint="h9gi-nx95",
        time_col="crash_date",
        base_domain="data.cityofnewyork.us",
    ),
    schema=collisions_schema,
    description="NYPD-reported motor vehicle collisions in NYC (2012-present). One row per crash event.",
)

nyc_motor_vehicle_collisions = collisions_pipeline.clean
