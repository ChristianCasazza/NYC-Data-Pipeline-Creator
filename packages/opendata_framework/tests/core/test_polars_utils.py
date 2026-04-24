import pytest
import polars as pl
from opendata_framework.core.polars_utils import (
    apply_schema_contract,
    safe_bool,
    safe_float,
    multi_parse_date,
    SchemaContract,
)


class TestSafeBool:
    def test_truthy_strings(self):
        df = pl.DataFrame({"val": ["1", "t", "true", "y", "yes", "TRUE", "Yes"]})
        result = df.select(safe_bool("val"))
        assert result["val"].to_list() == [True, True, True, True, True, True, True]

    def test_falsy_strings(self):
        df = pl.DataFrame({"val": ["0", "f", "false", "n", "no", "FALSE", "No"]})
        result = df.select(safe_bool("val"))
        assert result["val"].to_list() == [
            False,
            False,
            False,
            False,
            False,
            False,
            False,
        ]

    def test_null_values(self):
        df = pl.DataFrame({"val": [None, "maybe", "", "TRUE", "false"]})
        result = df.select(safe_bool("val"))
        assert result["val"][0] is None
        assert result["val"][1] is None
        assert result["val"][2] is None
        assert result["val"][3] is True
        assert result["val"][4] is False

    def test_numeric_input(self):
        df = pl.DataFrame({"val": [1, 0, 100, -5]})
        result = df.select(safe_bool("val"))
        assert result["val"][0] is True
        assert result["val"][1] is False
        assert result["val"][2] is None
        assert result["val"][3] is None

    def test_whitespace_handling(self):
        df = pl.DataFrame({"val": [" yes ", "  no  ", " true "]})
        result = df.select(safe_bool("val"))
        assert result["val"].to_list() == [True, False, True]


class TestSafeFloat:
    def test_plain_numbers(self):
        df = pl.DataFrame({"val": ["123.45", "0", "-42"]})
        result = df.select(safe_float("val"))
        assert result["val"].to_list() == [123.45, 0.0, -42.0]

    def test_currency_format(self):
        df = pl.DataFrame({"val": ["$1,234.56", "-$999.99", "$0.00"]})
        result = df.select(safe_float("val"))
        assert result["val"].to_list() == [1234.56, -999.99, 0.0]

    def test_whitespace_numbers(self):
        df = pl.DataFrame({"val": [" 42 ", "1 000"]})
        result = df.select(safe_float("val"))
        assert result["val"].to_list() == [42.0, 1000.0]

    def test_null_values(self):
        df = pl.DataFrame({"val": [None, "abc", "123"]})
        result = df.select(safe_float("val"))
        assert result["val"][0] is None
        assert result["val"][1] is None
        assert result["val"][2] == 123.0

    def test_non_string_input(self):
        df = pl.DataFrame({"val": [42, 0, -1]})
        result = df.select(safe_float("val"))
        assert result["val"].to_list() == [42.0, 0.0, -1.0]


class TestMultiParseDate:
    def test_iso_date(self):
        df = pl.DataFrame({"dt": ["2024-01-15"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dt.year()[0] == 2024
        assert result["dt"].dt.month()[0] == 1
        assert result["dt"].dt.day()[0] == 15

    def test_iso_datetime(self):
        df = pl.DataFrame({"dt": ["2024-06-15T10:30:00"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dt.year()[0] == 2024
        assert result["dt"].dt.hour()[0] == 10

    def test_utc_datetime(self):
        df = pl.DataFrame({"dt": ["2024-06-15T10:30:00Z"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dt.year()[0] == 2024

    def test_slash_date(self):
        df = pl.DataFrame({"dt": ["01/15/2024"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dt.year()[0] == 2024
        assert result["dt"].dt.month()[0] == 1

    def test_compact_date(self):
        df = pl.DataFrame({"dt": ["20240115"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dt.year()[0] == 2024

    def test_null_values(self):
        df = pl.DataFrame({"dt": [None, "2024-01-01"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"][0] is None
        assert result["dt"][1] is not None

    def test_timezone_aware_output(self):
        df = pl.DataFrame({"dt": ["2024-01-15"]})
        result = df.select(multi_parse_date("dt", output_timezone="UTC"))
        assert result["dt"].dtype.time_zone == "UTC"

    def test_default_timezone_is_nyc(self):
        df = pl.DataFrame({"dt": ["2024-01-15"]})
        result = df.select(multi_parse_date("dt"))
        assert result["dt"].dtype.time_zone == "America/New_York"


class TestApplySchemaContract:
    def test_strict_mode_keeps_only_contract_columns(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        contract = {"a": ("a", pl.Int64), "b": ("b", pl.Int64)}
        result = apply_schema_contract(df.lazy(), contract, drop_unknown=True)
        cols = result.collect().columns
        assert set(cols) == {"a", "b"}
        assert "c" not in cols

    def test_resilient_mode_passes_through_extras(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        contract = {"a": ("a", pl.Int64), "b": ("b", pl.Int64)}
        result = apply_schema_contract(df.lazy(), contract, drop_unknown=False)
        cols = result.collect().columns
        assert "c" in cols

    def test_column_rename(self):
        df = pl.DataFrame({"old_name": [1, 2, 3]})
        contract = {"old_name": ("new_name", pl.Int64)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert "new_name" in result.columns
        assert "old_name" not in result.columns

    def test_type_coercion_int_to_float(self):
        df = pl.DataFrame({"val": ["1", "2", "3"]})
        contract = {"val": ("val", pl.Float64)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["val"].dtype == pl.Float64
        assert result["val"].to_list() == [1.0, 2.0, 3.0]

    def test_type_coercion_float_from_string(self):
        df = pl.DataFrame({"val": ["1.5", "2.7", "$3,000"]})
        contract = {"val": ("val", pl.Float64)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["val"].dtype == pl.Float64
        assert result["val"][0] == 1.5
        assert result["val"][2] == 3000.0

    def test_type_coercion_boolean(self):
        df = pl.DataFrame({"val": ["true", "false", "yes", "0"]})
        contract = {"val": ("val", pl.Boolean)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["val"].to_list() == [True, False, True, False]

    def test_missing_column_creates_null(self):
        df = pl.DataFrame({"a": [1, 2]})
        contract = {"a": ("a", pl.Int64), "missing": ("missing", pl.Utf8)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["missing"][0] is None
        assert result["missing"].dtype == pl.Utf8

    def test_datetime_coercion(self):
        df = pl.DataFrame({"ts": ["2024-01-15T10:30:00Z"]})
        contract = {"ts": ("ts", pl.Datetime)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert isinstance(result["ts"].dtype, pl.Datetime)

    def test_date_coercion(self):
        df = pl.DataFrame({"d": ["2024-01-15"]})
        contract = {"d": ("d", pl.Date)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["d"].dtype == pl.Date

    def test_string_coercion_passthrough(self):
        df = pl.DataFrame({"s": ["hello", "world"]})
        contract = {"s": ("s", pl.Utf8)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["s"].to_list() == ["hello", "world"]

    def test_integer_coercion_with_commas(self):
        df = pl.DataFrame({"n": ["1,234", "5,678"]})
        contract = {"n": ("n", pl.Int64)}
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert result["n"].to_list() == [1234, 5678]

    def test_3_tuple_contract_needs_normalize(self):
        from opendata_framework.core.polars_utils import apply_schema_contract
        from opendata_framework.core.schema.contracts import normalize_schema

        df = pl.DataFrame({"src_col": [1, 2]})
        schema_3 = {"src_col": ("dst_col", pl.Int64, "A description")}
        contract, _ = normalize_schema(schema_3)
        result = apply_schema_contract(df.lazy(), contract).collect()
        assert "dst_col" in result.columns
        assert "src_col" not in result.columns
