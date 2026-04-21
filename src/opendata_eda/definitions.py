"""Dagster definitions entry point — loads component defs and wires resources."""
from pathlib import Path

import dagster as dg
from dagster import EnvVar, load_from_defs_folder

from opendata_framework.dagster import (
    SocrataResource,
    LandingIOManager,
    PolarsParquetIOManager,
    QueryStationResource,
)


def _get_resources() -> dict:
    return {
        "socrata": SocrataResource(api_token=EnvVar("SOCRATA_API_TOKEN")),
        "querystation": QueryStationResource(
            api_key=EnvVar("QUERYSTATION_API_KEY"),
            auth_url=EnvVar("AUTH_URL"),
        ),
        "landing_io_manager": LandingIOManager(base_path="./data/landing"),
        "clean_large_io_manager": PolarsParquetIOManager(base_path="./data/clean"),
        "clean_single_io_manager": PolarsParquetIOManager(base_path="./data/clean"),
        "clean_io_manager": PolarsParquetIOManager(base_path="./data/clean"),
        "analytics_io_manager": PolarsParquetIOManager(base_path="./data/clean"),
        "raw_large_io_manager": PolarsParquetIOManager(base_path="./data/landing"),
    }


def _load_defs() -> dg.Definitions:
    component_defs = load_from_defs_folder(
        path_within_project=Path(__file__).parent,
    )
    return dg.Definitions.merge(
        component_defs,
        dg.Definitions(resources=_get_resources()),
    )


defs = _load_defs()
