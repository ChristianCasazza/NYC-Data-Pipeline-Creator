# opendata_framework/dagster/standards.py
# Pydantic models defining the configuration contracts for asset generation.
# Updated to support both Time-Range and Equality-based partitioning strategies.

from typing import Any, Literal
from pydantic import BaseModel, Field

class SocrataIngestConfig(BaseModel):
    """
    Configuration for ingesting data from Socrata SODA APIs.
    """
    endpoint: str
    # The column used for filtering. 
    # For Time Mode: Must be a Timestamp or Floating Timestamp.
    # For Equality Mode: Can be a Year string, Fiscal Year, Agency Name, etc.
    partition_col: str = Field(alias="time_col") 
    
    # Strategy for generating the WHERE clause.
    # 'time' (Default): WHERE col >= start AND col < end
    # 'equality': WHERE col = 'partition_key'
    partition_filter_type: Literal["time", "equality"] = "time"
    
    order_field: str | None = None
    limit: int = 500_000
    base_domain: str = "data.ny.gov"

    def to_metadata(self) -> dict[str, Any]:
        """Helper to serialize for Dagster metadata."""
        return {"socrata_config": self.model_dump(by_alias=True)}


class HttpIngestConfig(BaseModel):
    """
    Configuration for ingesting a single file via HTTP.
    """
    url: str
    format: str = "parquet"
    user_agent: str = "Dagster OpenData Framework"

    def to_metadata(self) -> dict[str, Any]:
        return {"http_config": self.model_dump()}


class CheckbookIngestConfig(BaseModel):
    """
    Configuration for ingesting data from the Checkbook NYC XML API.

    Supports two filter strategies:
    - "date_range": Uses a range criterion (e.g. issue_date for Spending).
    - "fiscal_year": Uses a value criterion on the year field (e.g. for Budget).
    """
    type_of_data: str = "Spending"
    response_columns: list[str]
    filter_type: Literal["date_range", "fiscal_year"] = "date_range"
    filter_field: str = "issue_date"
    extra_criteria: list[dict[str, str]] = Field(default_factory=list)

    def to_metadata(self) -> dict[str, Any]:
        return {"checkbook_config": self.model_dump()}


class PolarsTransformConfig(BaseModel):
    """
    Configuration for standardized Polars cleaning steps.
    """
    rename_map: dict[str, str] = Field(default_factory=dict)
    date_cols: list[str] = Field(default_factory=list)
    int_cols: list[str] = Field(default_factory=list)
    float_cols: list[str] = Field(default_factory=list)
    bool_cols: list[str] = Field(default_factory=list)

    def to_metadata(self) -> dict[str, Any]:
        return {"transform_config": self.model_dump()}