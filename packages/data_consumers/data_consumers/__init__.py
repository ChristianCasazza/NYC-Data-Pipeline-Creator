"""
data-consumers: Dev helpers for ad-hoc querying of Parquet/DuckDB warehouses.

    from data_consumers import DuckDBWrapper, PolarsWrapper, RemoteDuckDBWrapper
"""

from data_consumers.duckdb_wrapper import DuckDBWrapper as DuckDBWrapper
from data_consumers.polars_wrapper import PolarsWrapper as PolarsWrapper
from data_consumers.remote_duckdb_wrapper import RemoteDuckDBWrapper as RemoteDuckDBWrapper

__all__ = [
    "DuckDBWrapper",
    "PolarsWrapper",
    "RemoteDuckDBWrapper",
]
