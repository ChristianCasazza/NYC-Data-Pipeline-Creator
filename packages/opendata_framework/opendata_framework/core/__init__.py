# opendata_framework/core/__init__.py
"""
Core data processing utilities — single import surface for cleaning and schema work.

Common usage:

    from opendata_framework.core import (
        apply_schema_contract,
        safe_bool,
        safe_float,
        multi_parse_date,
        load_partitioned_scans,
        build_table_schema,
        extract_polars_contract,
        SchemaContract,
    )
"""

# --- Polars utilities ---
from opendata_framework.core.polars_utils import (
    apply_schema_contract as apply_schema_contract,
    safe_bool as safe_bool,
    safe_float as safe_float,
    multi_parse_date as multi_parse_date,
    load_partitioned_scans as load_partitioned_scans,
    SchemaContract as SchemaContract,
)

# --- Schema builders ---
from opendata_framework.core.schema.contracts import (
    build_table_schema as build_table_schema,
    build_table_schema_from_contract as build_table_schema_from_contract,
    extract_polars_contract as extract_polars_contract,
    normalize_schema as normalize_schema,
    build_catalog_columns_metadata as build_catalog_columns_metadata,
)
from opendata_framework.core.schema.catalog import (
    polars_type_to_catalog_type as polars_type_to_catalog_type,
)

__all__ = [
    # Polars utilities
    "apply_schema_contract",
    "safe_bool",
    "safe_float",
    "multi_parse_date",
    "load_partitioned_scans",
    "SchemaContract",
    # Schema builders
    "build_table_schema",
    "build_table_schema_from_contract",
    "extract_polars_contract",
    "normalize_schema",
    "build_catalog_columns_metadata",
    "polars_type_to_catalog_type",
]
