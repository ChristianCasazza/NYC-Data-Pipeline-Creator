import polars as pl
import pytest

from opendata_framework.enrichments.geographic import (
    add_borough_key,
    add_location_flag,
    add_nyc_bbl,
    add_community_district_key,
    borough_key_expr,
    borough_code_expr,
    borough_name_expr,
)


class TestBoroughMapping:
    @pytest.mark.parametrize(
        "input_val,expected_key,expected_code,expected_name",
        [
            ("MANHATTAN", "manhattan", 1, "Manhattan"),
            ("manhattan", "manhattan", 1, "Manhattan"),
            ("Manhattan", "manhattan", 1, "Manhattan"),
            ("M", "manhattan", 1, "Manhattan"),
            ("BRONX", "bronx", 2, "Bronx"),
            ("X", "bronx", 2, "Bronx"),
            ("BK", "brooklyn", 3, "Brooklyn"),
            ("3", "brooklyn", 3, "Brooklyn"),
            ("QUEENS", "queens", 4, "Queens"),
            ("QN", "queens", 4, "Queens"),
            ("STATEN ISLAND", "staten_island", 5, "Staten Island"),
            ("SI", "staten_island", 5, "Staten Island"),
            ("R", "staten_island", 5, "Staten Island"),
            ("NEW YORK", "manhattan", 1, "Manhattan"),
            ("KINGS", "brooklyn", 3, "Brooklyn"),
            ("RICHMOND", "staten_island", 5, "Staten Island"),
            ("RICHMOND / STATEN ISLAND", "staten_island", 5, "Staten Island"),
        ],
    )
    def test_all_borough_variants(
        self, input_val, expected_key, expected_code, expected_name
    ):
        df = pl.DataFrame({"boro": [input_val]})
        result = add_borough_key(
            df.lazy(), "boro", key=True, code=True, canonical_name=True
        ).collect()
        assert result["borough_key"][0] == expected_key
        assert result["borough_code"][0] == expected_code
        assert result["borough_name"][0] == expected_name

    def test_unknown_borough_null(self):
        df = pl.DataFrame({"boro": ["UNKNOWN"]})
        result = add_borough_key(df.lazy(), "boro", key=True).collect()
        assert result["borough_key"][0] is None

    def test_whitespace_handling(self):
        df = pl.DataFrame({"boro": ["  MANHATTAN  "]})
        result = add_borough_key(df.lazy(), "boro", key=True).collect()
        assert result["borough_key"][0] == "manhattan"

    def test_null_values(self):
        df = pl.DataFrame({"boro": [None, "MANHATTAN"]})
        result = add_borough_key(df.lazy(), "boro", key=True).collect()
        assert result["borough_key"][1] == "manhattan"

    def test_selective_columns(self):
        df = pl.DataFrame({"boro": ["MANHATTAN"]})
        result = add_borough_key(
            df.lazy(), "boro", key=False, code=True, canonical_name=False
        ).collect()
        assert "borough_code" in result.columns
        assert "borough_key" not in result.columns
        assert "borough_name" not in result.columns

    def test_no_columns_selected(self):
        df = pl.DataFrame({"boro": ["MANHATTAN"]})
        result = add_borough_key(
            df.lazy(), "boro", key=False, code=False, canonical_name=False
        ).collect()
        assert "borough_key" not in result.columns


class TestBoroughExprs:
    def test_borough_key_expr(self):
        df = pl.DataFrame({"b": ["BRONX"]})
        result = df.select(borough_key_expr("b"))
        assert "borough_key" in result.columns
        assert result["borough_key"][0] == "bronx"

    def test_borough_code_expr(self):
        df = pl.DataFrame({"b": ["QUEENS"]})
        result = df.select(borough_code_expr("b"))
        assert "borough_code" in result.columns
        assert result["borough_code"][0] == 4

    def test_borough_name_expr(self):
        df = pl.DataFrame({"b": ["K"]})
        result = df.select(borough_name_expr("b"))
        assert "borough_name" in result.columns
        assert result["borough_name"][0] == "Brooklyn"


class TestLocationFlag:
    def test_valid_nyc_location(self):
        df = pl.DataFrame({"latitude": [40.7128], "longitude": [-74.006]})
        result = add_location_flag(df.lazy()).collect()
        assert result["has_location"][0] is True

    def test_null_coordinates(self):
        df = pl.DataFrame({"latitude": [None], "longitude": [-74.0]})
        result = add_location_flag(df.lazy()).collect()
        assert result["has_location"][0] is False

    def test_outside_nyc_bounds(self):
        df = pl.DataFrame({"latitude": [35.0], "longitude": [-120.0]})
        result = add_location_flag(df.lazy()).collect()
        assert result["has_location"][0] is False

    def test_no_bounds_check(self):
        df = pl.DataFrame({"latitude": [35.0], "longitude": [-120.0]})
        result = add_location_flag(df.lazy(), validate_nyc_bounds=False).collect()
        assert result["has_location"][0] is True

    def test_custom_columns(self):
        df = pl.DataFrame({"y": [40.7], "x": [-74.0]})
        result = add_location_flag(df.lazy(), lat_col="y", lon_col="x").collect()
        assert result["has_location"][0] is True

    def test_custom_alias(self):
        df = pl.DataFrame({"latitude": [40.7], "longitude": [-74.0]})
        result = add_location_flag(df.lazy(), alias="has_geo").collect()
        assert "has_geo" in result.columns


class TestNycBbl:
    def test_basic_bbl(self):
        df = pl.DataFrame({"boro": ["1"], "block": [12345], "lot": [67]})
        result = add_nyc_bbl(df.lazy()).collect()
        assert result["bbl"][0] == "1123450067"

    def test_small_block_lot(self):
        df = pl.DataFrame({"boro": ["2"], "block": [1], "lot": [1]})
        result = add_nyc_bbl(df.lazy()).collect()
        assert result["bbl"][0] == "2000010001"

    def test_custom_columns_and_alias(self):
        df = pl.DataFrame({"b": ["3"], "blk": [500], "lt": [12]})
        result = add_nyc_bbl(
            df.lazy(), boro_col="b", block_col="blk", lot_col="lt", alias="bbl_id"
        ).collect()
        assert "bbl_id" in result.columns
        assert result["bbl_id"][0] == "3005000012"


class TestCommunityDistrictKey:
    def test_with_borough(self):
        df = pl.DataFrame({"community_district": ["01"], "borough": ["MANHATTAN"]})
        result = add_community_district_key(df.lazy()).collect()
        assert result["community_district_key"][0] == "101"

    def test_with_numeric_borough(self):
        df = pl.DataFrame({"community_district": ["12"], "borough": ["3"]})
        result = add_community_district_key(df.lazy()).collect()
        assert result["community_district_key"][0] == "312"

    def test_without_borough(self):
        df = pl.DataFrame({"community_district": ["501"]})
        result = add_community_district_key(df.lazy(), borough_col=None).collect()
        assert result["community_district_key"][0] == "501"
