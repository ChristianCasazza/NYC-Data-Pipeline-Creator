import polars as pl
import pytest
from dagster import TableColumn

from opendata_framework.enrichments.builder import (
    StandardEnrichments,
    BoroughConfig,
    LocationConfig,
    TimestampConfig,
    TemporalConfig,
    CompletenessConfig,
)
from opendata_framework.enrichments.catalog import (
    borough_key_columns,
    location_flag_columns,
    record_timestamp_columns,
    temporal_columns,
    completeness_flag_columns,
    community_district_key_columns,
    nyc_bbl_columns,
    duplicate_marker_columns,
)


class TestBoroughConfig:
    def test_defaults(self):
        cfg = BoroughConfig(source_col="boro")
        assert cfg.source_col == "boro"
        assert cfg.key is True
        assert cfg.code is False
        assert cfg.canonical_name is False

    def test_full_config(self):
        cfg = BoroughConfig(
            source_col="borough", key=True, code=True, canonical_name=True
        )
        assert cfg.code is True
        assert cfg.canonical_name is True


class TestStandardEnrichments:
    def test_empty_enrichments_no_changes(self):
        e = StandardEnrichments()
        lf = pl.DataFrame({"x": [1, 2, 3]}).lazy()
        transform = e.build_transform_fn()
        result = transform(lf).collect()
        assert result.columns == ["x"]
        assert result.height == 3

    def test_borough_enrichment(self):
        e = StandardEnrichments(borough=BoroughConfig(source_col="boro"))
        lf = pl.DataFrame({"boro": ["MANHATTAN", "BROOKLYN"]}).lazy()
        transform = e.build_transform_fn()
        result = transform(lf).collect()
        assert "borough_key" in result.columns
        assert result["borough_key"][0] == "manhattan"
        assert result["borough_key"][1] == "brooklyn"

    def test_location_enrichment(self):
        e = StandardEnrichments(location=LocationConfig())
        lf = pl.DataFrame(
            {"latitude": [40.7, None], "longitude": [-74.0, -74.0]}
        ).lazy()
        transform = e.build_transform_fn()
        result = transform(lf).collect()
        assert "has_location" in result.columns

    def test_timestamp_enrichment(self):
        e = StandardEnrichments(
            timestamp=TimestampConfig(source_col="created_date", precision="day")
        )
        lf = pl.DataFrame({"created_date": ["2024-01-15"]}).lazy()
        transform = e.build_transform_fn()
        result = transform(lf).collect()
        assert "record_timestamp" in result.columns
        assert "record_date_precision" in result.columns

    def test_temporal_enrichment(self):
        e = StandardEnrichments(temporal=TemporalConfig(year=True, month=True))
        ts = (
            pl.Series("ts", ["2024-06-15"])
            .str.strptime(pl.Datetime, "%Y-%m-%d")
            .cast(pl.Datetime("us"))
            .dt.replace_time_zone("America/New_York")
        )
        df = pl.DataFrame({"ts": ts})
        # Need record_timestamp first
        e2 = StandardEnrichments(
            timestamp=TimestampConfig(source_col="ts", precision="day"),
            temporal=TemporalConfig(year=True, month=True),
        )
        transform = e2.build_transform_fn()
        result = transform(df.lazy()).collect()
        assert "year" in result.columns
        assert "month" in result.columns

    def test_completeness_enrichment(self):
        e = StandardEnrichments(
            completeness=CompletenessConfig(date_col="created_date")
        )
        lf = pl.DataFrame({"created_date": ["2024-01-15", None]}).lazy()
        transform = e.build_transform_fn()
        result = transform(lf).collect()
        assert "has_date" in result.columns

    def test_duplicate_derived_columns_detected(self):
        from dagster import TableColumn

        e = StandardEnrichments(
            location=LocationConfig(),
            extra_columns=[
                TableColumn(name="has_location", type="Boolean", description="dup")
            ],
        )
        with pytest.raises(ValueError, match="Duplicate derived column names"):
            e.build_derived_columns()

    def test_extra_columns(self):
        e = StandardEnrichments(
            extra_columns=[
                TableColumn(name="custom", type="Utf8", description="Custom")
            ]
        )
        cols = e.build_derived_columns()
        assert any(c.name == "custom" for c in cols)


class TestDerivedColumnsCatalog:
    def test_borough_key_columns(self):
        cols = borough_key_columns(key=True, code=True, canonical_name=True)
        assert len(cols) == 3
        assert cols[0].name == "borough_key"
        assert cols[1].name == "borough_code"
        assert cols[2].name == "borough_name"

    def test_location_flag_columns(self):
        cols = location_flag_columns()
        assert len(cols) == 1
        assert cols[0].name == "has_location"

    def test_record_timestamp_columns(self):
        cols = record_timestamp_columns()
        assert len(cols) == 2
        assert cols[0].name == "record_timestamp"
        assert cols[1].name == "record_date_precision"

    def test_temporal_columns(self):
        cols = temporal_columns(year=True, month=True, quarter=True)
        assert len(cols) == 3
        names = [c.name for c in cols]
        assert "year" in names
        assert "month" in names
        assert "quarter" in names

    def test_completeness_flag_columns(self):
        cols = completeness_flag_columns(date_col="crash_date")
        assert any(c.name == "has_date" for c in cols)

    def test_community_district_key_columns(self):
        cols = community_district_key_columns()
        assert len(cols) == 1
        assert cols[0].name == "community_district_key"

    def test_nyc_bbl_columns(self):
        cols = nyc_bbl_columns()
        assert len(cols) == 1
        assert cols[0].name == "bbl"

    def test_duplicate_marker_columns(self):
        cols = duplicate_marker_columns()
        assert len(cols) == 2
        assert cols[0].name == "_dedup_count"
        assert cols[1].name == "_dedup_rank"
