# opendata_framework/dagster/__init__.py
"""
Dagster integration facade — single import surface for pipeline authors.

Common usage:

    from opendata_framework.dagster import (
        create_socrata_pipeline,
        create_checkbook_pipeline,
        SocrataIngestConfig,
        CheckbookIngestConfig,
        yearly_partitions,
        monthly_partitions,
    )
"""

# --- Factories ---
from opendata_framework.dagster.factories import (
    CleanDiagnosticsConfig as CleanDiagnosticsConfig,
    PartitionGrain as PartitionGrain,
    PipelineResult as PipelineResult,
    create_checkbook_pipeline as create_checkbook_pipeline,
    create_socrata_pipeline as create_socrata_pipeline,
)

# --- Config models ---
from opendata_framework.dagster.standards import (
    CheckbookIngestConfig as CheckbookIngestConfig,
    HttpIngestConfig as HttpIngestConfig,
    PolarsTransformConfig as PolarsTransformConfig,
    SocrataIngestConfig as SocrataIngestConfig,
)

# --- Partition helpers ---
from opendata_framework.dagster.partitions import (
    monthly_partitions as monthly_partitions,
    yearly_partitions as yearly_partitions,
)

# --- Schema type ---
from opendata_framework.core import SchemaContract as SchemaContract

# --- SQL discovery ---
from opendata_framework.dagster.assets.sql_assets import (
    discover_sql_assets as discover_sql_assets,
)

# --- Resources ---
from opendata_framework.dagster.resources import (
    CheckbookNYCResource as CheckbookNYCResource,
    JsonIOManager as JsonIOManager,
    LandingIOManager as LandingIOManager,
    PolarsParquetIOManager as PolarsParquetIOManager,
    SocrataResource as SocrataResource,
)

__all__ = [
    # Factories
    "create_socrata_pipeline",
    "create_checkbook_pipeline",
    "PipelineResult",
    "CleanDiagnosticsConfig",
    "PartitionGrain",
    # Config models
    "SocrataIngestConfig",
    "CheckbookIngestConfig",
    "HttpIngestConfig",
    "PolarsTransformConfig",
    # Partitions
    "yearly_partitions",
    "monthly_partitions",
    # Schema
    "SchemaContract",
    # SQL
    "discover_sql_assets",
    # Resources
    "SocrataResource",
    "CheckbookNYCResource",
    "JsonIOManager",
    "LandingIOManager",
    "PolarsParquetIOManager",
]
