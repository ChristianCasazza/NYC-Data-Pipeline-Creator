import pytest
from opendata_framework.dagster.standards import (
    SocrataIngestConfig,
    HttpIngestConfig,
    CheckbookIngestConfig,
    PolarsTransformConfig,
)
from opendata_framework.dagster.partitions import yearly_partitions, monthly_partitions


class TestSocrataIngestConfig:
    def test_defaults(self):
        cfg = SocrataIngestConfig(endpoint="abc-123", time_col="created_date")
        assert cfg.endpoint == "abc-123"
        assert cfg.partition_filter_type == "time"
        assert cfg.limit == 500_000
        assert cfg.base_domain == "data.ny.gov"

    def test_alias(self):
        cfg = SocrataIngestConfig(endpoint="abc-123", time_col="created_date")
        assert cfg.partition_col == "created_date"

    def test_equality_mode(self):
        cfg = SocrataIngestConfig(
            endpoint="abc-123",
            time_col="borough",
            partition_filter_type="equality",
        )
        assert cfg.partition_filter_type == "equality"

    def test_to_metadata(self):
        cfg = SocrataIngestConfig(endpoint="abc-123", time_col="date")
        md = cfg.to_metadata()
        assert "socrata_config" in md
        assert md["socrata_config"]["endpoint"] == "abc-123"

    def test_custom_domain(self):
        cfg = SocrataIngestConfig(
            endpoint="abc-123", time_col="date", base_domain="data.cityofnewyork.us"
        )
        assert cfg.base_domain == "data.cityofnewyork.us"


class TestHttpIngestConfig:
    def test_defaults(self):
        cfg = HttpIngestConfig(url="https://example.com/data.parquet")
        assert cfg.format == "parquet"
        assert cfg.user_agent == "Dagster OpenData Framework"

    def test_to_metadata(self):
        cfg = HttpIngestConfig(url="https://example.com/data.csv", format="csv")
        md = cfg.to_metadata()
        assert md["http_config"]["url"] == "https://example.com/data.csv"
        assert md["http_config"]["format"] == "csv"


class TestCheckbookIngestConfig:
    def test_defaults(self):
        cfg = CheckbookIngestConfig(response_columns=["agency", "amount"])
        assert cfg.type_of_data == "Spending"
        assert cfg.filter_type == "date_range"
        assert cfg.filter_field == "issue_date"
        assert cfg.extra_criteria == []

    def test_fiscal_year_mode(self):
        cfg = CheckbookIngestConfig(
            response_columns=["agency"],
            filter_type="fiscal_year",
            filter_field="fiscal_year",
        )
        assert cfg.filter_type == "fiscal_year"

    def test_to_metadata(self):
        cfg = CheckbookIngestConfig(response_columns=["agency", "amount"])
        md = cfg.to_metadata()
        assert "checkbook_config" in md


class TestPolarsTransformConfig:
    def test_defaults(self):
        cfg = PolarsTransformConfig()
        assert cfg.rename_map == {}
        assert cfg.date_cols == []
        assert cfg.int_cols == []
        assert cfg.float_cols == []
        assert cfg.bool_cols == []

    def test_to_metadata(self):
        cfg = PolarsTransformConfig(rename_map={"old": "new"})
        md = cfg.to_metadata()
        assert md["transform_config"]["rename_map"] == {"old": "new"}


class TestYearlyPartitions:
    def test_creates_partition_definition(self):
        p = yearly_partitions("2020")
        assert p is not None

    def test_with_end_offset(self):
        p = yearly_partitions("2020", end_offset=2)
        assert p is not None


class TestMonthlyPartitions:
    def test_creates_partition_definition(self):
        p = monthly_partitions("2020-01-01")
        assert p is not None

    def test_with_end_date(self):
        p = monthly_partitions("2020-01-01", end_date="2024-12-31")
        assert p is not None
