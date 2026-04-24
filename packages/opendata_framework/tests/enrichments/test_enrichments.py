import polars as pl
import pytest

from opendata_framework.enrichments.text import (
    clean_text_expr,
    apply_literal_replacements,
    split_and_clean_list,
)
from opendata_framework.enrichments.flags import add_completeness_flags
from opendata_framework.enrichments.numeric import sum_components, compute_rate
from opendata_framework.enrichments.mapping import (
    map_with_default,
    ordered_regex_classification,
    collect_matching_labels,
    assemble_annotations,
)
from opendata_framework.enrichments.ids import stable_string_hash
from opendata_framework.enrichments.layout import reorder_columns
from opendata_framework.enrichments.dedup import add_duplicate_markers


class TestCleanTextExpr:
    def test_uppercase(self):
        df = pl.DataFrame({"t": ["  hello world  "]})
        result = df.select(clean_text_expr("t", uppercase=True))
        assert result["t"][0] == "HELLO WORLD"

    def test_no_uppercase(self):
        df = pl.DataFrame({"t": ["  hello world  "]})
        result = df.select(clean_text_expr("t", uppercase=False))
        assert result["t"][0] == "hello world"

    def test_collapse_whitespace(self):
        df = pl.DataFrame({"t": ["hello   world\n\nfoo"]})
        result = df.select(clean_text_expr("t", uppercase=False))
        assert result["t"][0] == "hello world foo"

    def test_custom_alias(self):
        df = pl.DataFrame({"t": ["hello"]})
        result = df.select(clean_text_expr("t", alias="clean_t"))
        assert "clean_t" in result.columns

    def test_null_passthrough(self):
        df = pl.DataFrame({"t": [None, "  hello  "]})
        result = df.select(clean_text_expr("t", uppercase=True))
        assert result["t"][0] is None
        assert result["t"][1] == "HELLO"


class TestApplyLiteralReplacements:
    def test_single_replacement(self):
        df = pl.DataFrame({"t": ["NYC"]})
        expr = apply_literal_replacements(pl.col("t"), {"NYC": "New York City"})
        result = df.select(expr.alias("t"))
        assert result["t"][0] == "New York City"

    def test_multiple_ordered_replacements(self):
        df = pl.DataFrame({"t": ["A"]})
        expr = apply_literal_replacements(pl.col("t"), {"A": "B", "B": "C"})
        result = df.select(expr.alias("t"))
        assert result["t"][0] == "C"  # ordered application


class TestSplitAndCleanList:
    def test_basic_split(self):
        df = pl.DataFrame({"t": ["a, b, c"]})
        result = df.select(split_and_clean_list(pl.col("t")).alias("items"))
        items = result["items"][0].to_list()
        assert items == ["a", "b", "c"]

    def test_empty_elements_filtered(self):
        df = pl.DataFrame({"t": ["a,,b,,c"]})
        result = df.select(split_and_clean_list(pl.col("t")).alias("items"))
        items = result["items"][0].to_list()
        assert items == ["a", "b", "c"]

    def test_custom_delimiter(self):
        df = pl.DataFrame({"t": ["x|y|z"]})
        result = df.select(
            split_and_clean_list(pl.col("t"), delimiter="|").alias("items")
        )
        items = result["items"][0].to_list()
        assert items == ["x", "y", "z"]


class TestAddCompletenessFlags:
    def test_date_flag(self):
        df = pl.DataFrame({"d": ["2024-01-01", None]})
        result = add_completeness_flags(df.lazy(), date_col="d").collect()
        assert result["has_date"][0] is True
        assert result["has_date"][1] is None or result["has_date"][1] is False

    def test_location_flag(self):
        df = pl.DataFrame({"lat": [40.7, None], "lon": [-74.0, -74.0]})
        result = add_completeness_flags(
            df.lazy(), lat_col="lat", lon_col="lon"
        ).collect()
        assert result["has_location"][0] is True
        assert result["has_location"][1] is False

    def test_geo_id_flag(self):
        df = pl.DataFrame({"bbl": ["1234567890", None], "bin": [None, "1234567"]})
        result = add_completeness_flags(df.lazy(), geo_id_cols=["bbl", "bin"]).collect()
        assert result["has_geo_ids"][0] is True
        assert result["has_geo_ids"][1] is True

    def test_custom_flags(self):
        df = pl.DataFrame({"a": [1, None], "b": [2, None]})
        result = add_completeness_flags(
            df.lazy(), custom_flags={"both_present": ["a", "b"]}
        ).collect()
        assert result["both_present"][0] is True
        assert result["both_present"][1] is False

    def test_no_flags_requested(self):
        df = pl.DataFrame({"x": [1]})
        result = add_completeness_flags(df.lazy()).collect()
        assert result.columns == ["x"]


class TestSumComponents:
    def test_basic_sum(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "c": [5, 6]})
        result = df.select(sum_components(["a", "b", "c"], "total"))
        assert result["total"].to_list() == [9, 12]

    def test_null_handling(self):
        df = pl.DataFrame({"a": [1, None], "b": [None, 4]})
        result = df.select(sum_components(["a", "b"], "total"))
        assert result["total"][0] == 1.0
        assert result["total"][1] == 4.0


class TestComputeRate:
    def test_basic_rate(self):
        df = pl.DataFrame({"num": [10, 20], "den": [100, 200]})
        result = df.select(compute_rate("num", "den", "rate"))
        assert result["rate"][0] == pytest.approx(0.1)
        assert result["rate"][1] == pytest.approx(0.1)

    def test_null_on_zero(self):
        df = pl.DataFrame({"num": [10], "den": [0]})
        result = df.select(compute_rate("num", "den", "rate", null_on_zero=True))
        assert result["rate"][0] is None

    def test_scale(self):
        df = pl.DataFrame({"seconds": [120], "minutes": [1]})
        result = df.select(compute_rate("seconds", "minutes", "rate", scale=1 / 60))
        assert result["rate"][0] == pytest.approx(2.0)

    def test_rounding(self):
        df = pl.DataFrame({"num": [1], "den": [3]})
        result = df.select(compute_rate("num", "den", "rate", round_digits=2))
        assert result["rate"][0] == pytest.approx(0.33, abs=0.01)


class TestMapWithDefault:
    def test_mapping(self):
        df = pl.DataFrame({"x": ["A", "B", "C"]})
        result = df.select(
            map_with_default(
                pl.col("x"), {"A": "Alpha", "B": "Beta"}, default="Unknown"
            )
        )
        assert result["x"].to_list() == ["Alpha", "Beta", "Unknown"]


class TestOrderedRegexClassification:
    def test_first_match_wins(self):
        df = pl.DataFrame({"x": ["heat emergency", "cold emergency", "normal day"]})
        result = df.select(
            ordered_regex_classification(
                pl.col("x"),
                [("heat", r"(?i)heat"), ("cold", r"(?i)cold")],
                default="other",
            ).alias("class")
        )
        assert result["class"].to_list() == ["heat", "cold", "other"]

    def test_no_rules(self):
        df = pl.DataFrame({"x": ["anything"]})
        result = df.select(
            ordered_regex_classification(pl.col("x"), [], default="default").alias(
                "class"
            )
        )
        assert result["class"][0] == "default"


class TestCollectMatchingLabels:
    def test_multiple_matches(self):
        df = pl.DataFrame({"x": ["heat and cold emergency"]})
        result = df.select(
            collect_matching_labels(
                pl.col("x"),
                [("heat", r"(?i)heat"), ("cold", r"(?i)cold")],
            ).alias("labels")
        )
        labels = result["labels"][0]
        assert "heat" in labels
        assert "cold" in labels

    def test_no_matches_null(self):
        df = pl.DataFrame({"x": ["nothing here"]})
        result = df.select(
            collect_matching_labels(
                pl.col("x"),
                [("heat", r"(?i)heat")],
            ).alias("labels")
        )
        assert result["labels"][0] is None


class TestAssembleAnnotations:
    def test_basic(self):
        df = pl.DataFrame({"x": [100, 50]})
        result = df.select(
            assemble_annotations(
                [(pl.col("x") > 75, "high"), (pl.col("x") > 25, "medium")],
            ).alias("notes")
        )
        assert "high" in result["notes"][0]
        assert "medium" in result["notes"][1]

    def test_no_matches_null(self):
        df = pl.DataFrame({"x": [1]})
        result = df.select(
            assemble_annotations(
                [],
            ).alias("notes")
        )
        assert result["notes"][0] is None


class TestStableStringHash:
    def test_deterministic(self):
        df = pl.DataFrame({"a": ["hello", "hello"], "b": ["world", "world"]})
        result = df.select(stable_string_hash(["a", "b"]).alias("hash"))
        assert result["hash"][0] == result["hash"][1]

    def test_different_inputs(self):
        df = pl.DataFrame({"a": ["hello", "goodbye"], "b": ["world", "world"]})
        result = df.select(stable_string_hash(["a", "b"]).alias("hash"))
        assert result["hash"][0] != result["hash"][1]

    def test_null_handling(self):
        df = pl.DataFrame({"a": ["hello", None], "b": ["world", "world"]})
        result = df.select(stable_string_hash(["a", "b"]).alias("hash"))
        assert result["hash"][1] is not None


class TestReorderColumns:
    def test_basic(self):
        df = pl.DataFrame({"c": [1], "a": [2], "b": [3]})
        result = reorder_columns(df.lazy(), priority_cols=["a", "b"]).collect()
        assert result.columns == ["a", "b", "c"]

    def test_missing_priority_ignored(self):
        df = pl.DataFrame({"c": [1], "a": [2]})
        result = reorder_columns(df.lazy(), priority_cols=["a", "b", "z"]).collect()
        assert result.columns == ["a", "c"]

    def test_no_priority_returns_same(self):
        df = pl.DataFrame({"x": [1], "y": [2]})
        result = reorder_columns(df.lazy(), priority_cols=[]).collect()
        assert result.columns == ["x", "y"]


class TestAddDuplicateMarkers:
    def test_basic_dedup(self):
        df = pl.DataFrame({"id": ["A", "A", "B"], "val": [20, 10, 30]})
        result = add_duplicate_markers(
            df.lazy(), key_cols=["id"], rank_col="val"
        ).collect()
        assert result["_dedup_count"][0] == 2
        assert result["_dedup_count"][1] == 2
        assert result["_dedup_count"][2] == 1
        assert result["_dedup_rank"][0] == 1
        assert result["_dedup_rank"][1] == 2

    def test_custom_column_names(self):
        df = pl.DataFrame({"id": ["A", "A"], "ts": [1, 2]})
        result = add_duplicate_markers(
            df.lazy(),
            key_cols=["id"],
            rank_col="ts",
            count_col="cnt",
            rank_out_col="rnk",
        ).collect()
        assert "cnt" in result.columns
        assert "rnk" in result.columns
