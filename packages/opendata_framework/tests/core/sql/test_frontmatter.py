import pytest
from opendata_framework.core.sql.frontmatter import split_front_matter


class TestSplitFrontMatter:
    def test_valid_frontmatter(self):
        raw = """/*---
name: my_asset
deps:
  - upstream_a
  - upstream_b
---*/
SELECT * FROM upstream_a
"""
        meta, sql = split_front_matter(raw)
        assert meta["name"] == "my_asset"
        assert meta["deps"] == ["upstream_a", "upstream_b"]
        assert sql.strip() == "SELECT * FROM upstream_a"

    def test_no_frontmatter(self):
        raw = "SELECT * FROM foo"
        meta, sql = split_front_matter(raw)
        assert meta == {}
        assert sql.strip() == "SELECT * FROM foo"

    def test_empty_frontmatter(self):
        raw = """/*---\n---*/\nSELECT 1"""
        meta, sql = split_front_matter(raw)
        assert meta == {}
        assert sql.strip() == "SELECT 1"

    def test_frontmatter_with_multiline_sql(self):
        raw = """/*---
name: test
---*/
SELECT
  a,
  b
FROM t
WHERE x > 10
"""
        meta, sql = split_front_matter(raw)
        assert meta["name"] == "test"
        assert "SELECT" in sql
        assert "WHERE" in sql

    def test_frontmatter_with_strings_and_tags(self):
        raw = """/*---
name: my_query
description: "A test query"
tags:
  domain: transit
  source: querystation
---*/
SELECT * FROM lake.transit.ridership
"""
        meta, sql = split_front_matter(raw)
        assert meta["name"] == "my_query"
        assert meta["description"] == "A test query"
        assert meta["tags"]["domain"] == "transit"
        assert meta["tags"]["source"] == "querystation"

    def test_frontmatter_only_returns_empty_sql(self):
        raw = "/*---\nname: empty\n---*/\n"
        meta, sql = split_front_matter(raw)
        assert meta["name"] == "empty"
        assert sql == ""

    def test_whitespace_before_frontmatter(self):
        raw = "\n\n  /*---\nname: leading\n---*/\nSELECT 1"
        meta, sql = split_front_matter(raw)
        assert meta["name"] == "leading"
        assert sql.strip() == "SELECT 1"

    def test_yaml_list_in_frontmatter(self):
        raw = """/*---
name: test
deps: [a, b, c]
---*/
SELECT 1
"""
        meta, sql = split_front_matter(raw)
        assert meta["deps"] == ["a", "b", "c"]

    def test_numeric_values(self):
        raw = """/*---
name: test
count: 42
ratio: 3.14
---*/
SELECT 1
"""
        meta, sql = split_front_matter(raw)
        assert meta["count"] == 42
        assert meta["ratio"] == pytest.approx(3.14)
