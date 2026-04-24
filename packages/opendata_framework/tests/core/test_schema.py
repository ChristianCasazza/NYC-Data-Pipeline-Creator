import pytest
import polars as pl
from dagster import TableColumn

from opendata_framework.core.schema.contracts import (
    normalize_schema,
    extract_polars_contract,
    build_table_schema,
    build_table_schema_from_contract,
    build_catalog_columns_metadata,
)
from opendata_framework.core.schema.catalog import polars_type_to_catalog_type


class TestNormalizeSchema:
    def test_2tuple_preserved(self):
        schema = {"a": ("a", pl.Utf8), "b": ("b", pl.Int64)}
        contract, is_3 = normalize_schema(schema)
        assert is_3 is False
        assert contract == schema

    def test_3tuple_stripped(self):
        schema = {"a": ("a", pl.Utf8, "desc"), "b": ("b", pl.Int64, "desc2")}
        contract, is_3 = normalize_schema(schema)
        assert is_3 is True
        assert contract == {"a": ("a", pl.Utf8), "b": ("b", pl.Int64)}

    def test_mixed_lengths_raise(self):
        schema = {"a": ("a", pl.Utf8), "b": ("b", pl.Int64, "desc")}
        with pytest.raises(ValueError, match="mixed tuple lengths"):
            normalize_schema(schema)

    def test_single_entry_schema(self):
        schema = {"col": ("out", pl.Float64)}
        contract, is_3 = normalize_schema(schema)
        assert is_3 is False
        assert contract["col"] == ("out", pl.Float64)


class TestExtractPolarsContract:
    def test_extracts_2tuple_from_3tuple(self):
        schema = {"a": ("alpha", pl.Utf8, "desc"), "b": ("beta", pl.Int64, "desc2")}
        result = extract_polars_contract(schema)
        assert result == {"a": ("alpha", pl.Utf8), "b": ("beta", pl.Int64)}


class TestBuildTableSchema:
    def test_basic_schema(self):
        schema = {
            "a": ("alpha", pl.Utf8, "A column"),
            "b": ("beta", pl.Int64, "B column"),
        }
        tbl = build_table_schema(schema)
        assert len(tbl.columns) == 2
        assert tbl.columns[0].name == "alpha"
        assert tbl.columns[1].name == "beta"

    def test_with_derived_columns(self):
        schema = {"a": ("alpha", pl.Utf8, "A column")}
        derived = [TableColumn(name="extra", type="Utf8", description="Derived")]
        tbl = build_table_schema(schema, derived_columns=derived)
        assert len(tbl.columns) == 2
        assert tbl.columns[1].name == "extra"

    def test_with_insert_after(self):
        schema = {"a": ("alpha", pl.Utf8, "A column")}
        insert = [TableColumn(name="inserted", type="Utf8", description="Inserted")]
        tbl = build_table_schema(schema, insert_after={"alpha": insert})
        assert len(tbl.columns) == 2
        assert tbl.columns[0].name == "alpha"
        assert tbl.columns[1].name == "inserted"


class TestBuildTableSchemaFromContract:
    def test_2tuple_schema(self):
        schema = {"a": ("alpha", pl.Utf8), "b": ("beta", pl.Int64)}
        tbl = build_table_schema_from_contract(schema)
        assert len(tbl.columns) == 2
        assert tbl.columns[0].name == "alpha"
        assert tbl.columns[0].description == ""

    def test_3_tuple_schema(self):
        schema = {
            "a": ("alpha", pl.Utf8, "A description"),
            "b": ("beta", pl.Int64, "B description"),
        }
        tbl = build_table_schema_from_contract(schema)
        assert tbl.columns[0].description == "A description"
        assert tbl.columns[1].description == "B description"

    def test_with_derived_columns(self):
        schema = {"a": ("alpha", pl.Utf8)}
        derived = [TableColumn(name="derived", type="Utf8", description="Extra")]
        tbl = build_table_schema_from_contract(schema, derived_columns=derived)
        assert len(tbl.columns) == 2
        assert tbl.columns[1].name == "derived"


class TestBuildCatalogColumnsMetadata:
    def test_basic(self):
        schema = {
            "a": ("alpha", pl.Utf8, "A column"),
            "b": ("beta", pl.Int64, "B column"),
        }
        cols = build_catalog_columns_metadata(schema)
        assert len(cols) == 2
        assert cols[0]["name"] == "alpha"
        assert cols[0]["api_name"] == "a"

    def test_with_derived_columns(self):
        schema = {"a": ("alpha", pl.Utf8, "A column")}
        derived = [{"name": "extra", "type": "string", "description": "Derived"}]
        cols = build_catalog_columns_metadata(schema, derived_columns=derived)
        assert len(cols) == 2
        assert cols[1]["name"] == "extra"

    def test_with_insert_after(self):
        schema = {"a": ("alpha", pl.Utf8, "A column")}
        insert = [{"name": "inserted", "type": "string", "description": "Inserted"}]
        cols = build_catalog_columns_metadata(schema, insert_after={"alpha": insert})
        assert len(cols) == 2
        assert cols[1]["name"] == "inserted"


class TestPolarsTypeToCatalogType:
    def test_utf8(self):
        assert polars_type_to_catalog_type(pl.Utf8) == "string"

    def test_string(self):
        assert polars_type_to_catalog_type(pl.String) == "string"

    def test_datetime(self):
        assert polars_type_to_catalog_type(pl.Datetime) == "date"

    def test_date(self):
        assert polars_type_to_catalog_type(pl.Date) == "date"

    def test_float64(self):
        assert polars_type_to_catalog_type(pl.Float64) == "number"

    def test_int64(self):
        assert polars_type_to_catalog_type(pl.Int64) == "number"

    def test_boolean(self):
        assert polars_type_to_catalog_type(pl.Boolean) == "boolean"

    def test_unknown_type(self):
        assert polars_type_to_catalog_type(pl.List) == "unknown"
