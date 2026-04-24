import polars as pl
import pytest

from opendata_framework.enrichments.temporal import (
    parse_string_date,
    add_record_timestamp,
    add_temporal_columns,
    compute_duration,
    enforce_timezone,
)


class TestParseStringDate:
    def test_iso_format(self):
        df = pl.DataFrame({"d": ["2024-01-15"]})
        result = df.select(parse_string_date("d", "%Y-%m-%d"))
        assert result["d"].dtype == pl.Date
        assert result["d"][0].strftime("%Y-%m-%d") == "2024-01-15"

    def test_with_pre_replace(self):
        df = pl.DataFrame({"d": ["2026 / 02"]})
        result = df.select(
            parse_string_date("d", "%Y-%m-%d", pre_replace={" / ": "-"}, suffix="-01")
        )
        assert result["d"][0] is not None
        assert result["d"][0].strftime("%Y-%m-%d") == "2026-02-01"

    def test_with_suffix(self):
        df = pl.DataFrame({"d": ["2026-02"]})
        result = df.select(parse_string_date("d", "%Y-%m-%d", suffix="-01"))
        assert result["d"][0].strftime("%Y-%m-%d") == "2026-02-01"

    def test_custom_alias(self):
        df = pl.DataFrame({"d": ["2024-01-15"]})
        result = df.select(parse_string_date("d", "%Y-%m-%d", alias="parsed_date"))
        assert "parsed_date" in result.columns

    def test_invalid_date_null(self):
        df = pl.DataFrame({"d": ["not_a_date"]})
        result = df.select(parse_string_date("d", "%Y-%m-%d"))
        assert result["d"][0] is None


class TestAddRecordTimestamp:
    def test_day_precision(self):
        df = pl.DataFrame({"d": ["2024-01-15"]})
        result = add_record_timestamp(
            df.lazy(),
            pl.col("d").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            "day",
        ).collect()
        assert "record_timestamp" in result.columns
        assert "record_date_precision" in result.columns
        assert result["record_date_precision"][0] == "day"
        assert result["record_timestamp"].dtype.time_zone == "America/New_York"

    def test_datetime_precision(self):
        df = pl.DataFrame({"ts": ["2024-06-15T10:30:00"]})
        result = add_record_timestamp(
            df.lazy(),
            pl.col("ts").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S", strict=False),
            "datetime",
        ).collect()
        assert result["record_date_precision"][0] == "datetime"

    def test_year_precision(self):
        df = pl.DataFrame({"yr": [2024]})
        result = add_record_timestamp(
            df.lazy(), pl.col("yr").cast(pl.Int32), "year"
        ).collect()
        assert result["record_date_precision"][0] == "year"

    def test_tz_aware_input(self):
        df = pl.DataFrame({"ts": ["2024-06-15T10:30:00+00:00"]})
        result = add_record_timestamp(
            df.lazy(),
            pl.col("ts").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z", strict=False),
            "datetime",
        ).collect()
        assert result["record_timestamp"].dtype.time_zone == "America/New_York"


class TestAddTemporalColumns:
    def test_year_and_month(self):
        df = (
            pl.DataFrame({"d": ["2024-06-15"]})
            .with_columns(pl.col("d").str.strptime(pl.Date, "%Y-%m-%d").alias("d"))
            .with_columns(
                pl.col("d")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("America/New_York")
                .alias("ts")
            )
        )
        result = add_temporal_columns(
            df.lazy(), "ts", year=True, month=True, quarter=True
        ).collect()
        assert result["year"][0] == 2024
        assert result["month"][0] == 6
        assert result["quarter"][0] == 2

    def test_fiscal_year(self):
        df = (
            pl.DataFrame({"d": ["2024-07-01", "2024-06-30"]})
            .with_columns(pl.col("d").str.strptime(pl.Date, "%Y-%m-%d").alias("d"))
            .with_columns(
                pl.col("d")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("America/New_York")
                .alias("ts")
            )
        )
        result = add_temporal_columns(df.lazy(), "ts", fiscal_year=True).collect()
        assert result["fiscal_year"][0] == 2025
        assert result["fiscal_year"][1] == 2024

    def test_season(self):
        df = (
            pl.DataFrame({"m": [1, 4, 7, 10]})
            .with_columns(pl.date(2024, pl.col("m"), 1).alias("d"))
            .with_columns(
                pl.col("d")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("America/New_York")
                .alias("ts")
            )
        )
        result = add_temporal_columns(df.lazy(), "ts", season=True).collect()
        assert result["season"][0] == "winter"
        assert result["season"][1] == "spring"
        assert result["season"][2] == "summer"
        assert result["season"][3] == "fall"

    def test_prefix(self):
        df = (
            pl.DataFrame({"d": ["2024-06-15"]})
            .with_columns(pl.col("d").str.strptime(pl.Date, "%Y-%m-%d").alias("d"))
            .with_columns(
                pl.col("d")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("America/New_York")
                .alias("ts")
            )
        )
        result = add_temporal_columns(
            df.lazy(), "ts", year=True, prefix="flood_"
        ).collect()
        assert "flood_year" in result.columns
        assert "year" not in result.columns

    def test_no_columns_returns_same(self):
        df = pl.DataFrame({"d": [1]})
        result = add_temporal_columns(
            df.lazy(), "d", year=False, month=False, quarter=False
        ).collect()
        assert result.columns == ["d"]


class TestComputeDuration:
    def test_basic_hours(self):
        df = pl.DataFrame(
            {
                "start": ["2024-01-01T10:00:00", "2024-01-01T10:00:00"],
                "end": ["2024-01-01T12:00:00", "2024-01-01T08:00:00"],
            }
        ).with_columns(
            pl.col("start")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .alias("start"),
            pl.col("end").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").alias("end"),
        )
        result = compute_duration("start", "end", unit="hours")
        df_result = df.select(result)
        assert df_result["start_to_end_hours"][0] == pytest.approx(2.0)
        assert df_result["start_to_end_hours"][1] == pytest.approx(-2.0)

    def test_minutes(self):
        df = pl.DataFrame(
            {
                "s": ["2024-01-01T00:00:00"],
                "e": ["2024-01-01T00:30:00"],
            }
        ).with_columns(
            pl.col("s").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").alias("s"),
            pl.col("e").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").alias("e"),
        )
        result = compute_duration("s", "e", unit="minutes")
        df_result = df.select(result)
        assert df_result["s_to_e_minutes"][0] == pytest.approx(30.0)

    def test_flag_negative(self):
        df = pl.DataFrame(
            {
                "start": ["2024-01-01T12:00:00", "2024-01-01T12:00:00"],
                "end": ["2024-01-01T13:00:00", "2024-01-01T11:00:00"],
            }
        ).with_columns(
            pl.col("start")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S")
            .alias("start"),
            pl.col("end").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").alias("end"),
        )
        result = compute_duration("start", "end", unit="hours", flag_negative=True)
        assert isinstance(result, list)
        assert len(result) == 2

    def test_invalid_unit(self):
        with pytest.raises(ValueError, match="unit must be one of"):
            compute_duration("a", "b", unit="weeks")


class TestEnforceTimezone:
    def test_naive_datetime_gets_tz(self):
        df = pl.DataFrame({"ts": ["2024-06-15 10:30:00"]}).with_columns(
            pl.col("ts").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias("ts")
        )
        result = enforce_timezone(df.lazy(), ["ts"]).collect()
        assert result["ts"].dtype.time_zone == "America/New_York"

    def test_already_tz_aware_converted(self):
        df = pl.DataFrame({"ts": ["2024-06-15T10:30:00+00:00"]}).with_columns(
            pl.col("ts").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%z").alias("ts")
        )
        result = enforce_timezone(df.lazy(), ["ts"]).collect()
        assert result["ts"].dtype.time_zone == "America/New_York"

    def test_missing_column_skipped(self):
        df = pl.DataFrame({"a": [1]})
        result = enforce_timezone(df.lazy(), ["nonexistent"]).collect()
        assert result.columns == ["a"]

    def test_custom_target_tz(self):
        df = pl.DataFrame({"ts": ["2024-06-15 10:30:00"]}).with_columns(
            pl.col("ts").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S").alias("ts")
        )
        result = enforce_timezone(df.lazy(), ["ts"], target_tz="UTC").collect()
        assert result["ts"].dtype.time_zone == "UTC"
