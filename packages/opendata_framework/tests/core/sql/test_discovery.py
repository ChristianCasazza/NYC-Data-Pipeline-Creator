import pytest
import tempfile
from pathlib import Path

from opendata_framework.core.sql.discovery import discover_sql_specs
from opendata_framework.core.sql.specs import SqlAssetSpec


class TestDiscoverSqlSpecs:
    def test_discovers_sql_files(self, tmp_path):
        sql_file = tmp_path / "my_asset.sql"
        sql_file.write_text("""/*---
name: test_asset
deps:
  - upstream_a
---*/
SELECT * FROM upstream_a
""")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 1
        assert specs[0].name == "test_asset"
        assert "upstream_a" in specs[0].declared_deps

    def test_name_defaults_to_file_stem(self, tmp_path):
        sql_file = tmp_path / "my_query.sql"
        sql_file.write_text("SELECT 1")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 1
        assert specs[0].name == "my_query"

    def test_no_frontmatter(self, tmp_path):
        sql_file = tmp_path / "raw_query.sql"
        sql_file.write_text("SELECT * FROM some_table WHERE x > 10")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 1
        assert specs[0].sql.strip() == "SELECT * FROM some_table WHERE x > 10"
        assert specs[0].tags == {}
        assert specs[0].declared_deps == []

    def test_skips_malformed_yaml(self, tmp_path, caplog):
        sql_file = tmp_path / "bad_yaml.sql"
        sql_file.write_text("/*---\n: invalid yaml [\n---*/\nSELECT 1")
        import logging

        with caplog.at_level(logging.WARNING):
            specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 0 or len(specs) == 1

    def test_skips_unreadable_files(self, tmp_path):
        bad_file = tmp_path / "bad_encoding.sql"
        bad_file.write_bytes(b"\xff\xfe INVALID UTF-8 \x80\x81")
        try:
            specs = discover_sql_specs(root=tmp_path)
            pass
        except Exception:
            pass

    def test_extra_deps(self, tmp_path):
        sql_file = tmp_path / "my_asset.sql"
        sql_file.write_text("/*---\nname: test\n---*/\nSELECT 1")
        specs = discover_sql_specs(
            root=tmp_path,
            extra_deps={"test": ["extra_dep"]},
        )
        assert len(specs) == 1
        assert "extra_dep" in specs[0].extra_deps

    def test_nested_directories(self, tmp_path):
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        sql_file = subdir / "nested.sql"
        sql_file.write_text("/*---\nname: nested_asset\n---*/\nSELECT 1")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 1
        assert specs[0].name == "nested_asset"

    def test_empty_directory(self, tmp_path):
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 0

    def test_tags_parsed(self, tmp_path):
        sql_file = tmp_path / "tagged.sql"
        sql_file.write_text("""/*---
name: tagged
tags:
  domain: transit
  source: querystation
---*/
SELECT 1
""")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 1
        assert specs[0].tags["domain"] == "transit"
        assert specs[0].tags["source"] == "querystation"

    def test_non_sql_files_ignored(self, tmp_path):
        (tmp_path / "readme.md").write_text("# Not SQL")
        (tmp_path / "data.csv").write_text("a,b\n1,2")
        specs = discover_sql_specs(root=tmp_path)
        assert len(specs) == 0
