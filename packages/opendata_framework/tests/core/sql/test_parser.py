import pytest
from opendata_framework.core.sql.parser import (
    extract_table_names,
    extract_qualified_table_names,
)


class TestExtractTableNames:
    def test_simple_select(self):
        sql = "SELECT * FROM my_table"
        assert extract_table_names(sql) == {"my_table"}

    def test_join(self):
        sql = "SELECT * FROM orders JOIN customers ON orders.cid = customers.id"
        result = extract_table_names(sql)
        assert "orders" in result
        assert "customers" in result

    def test_cte_excluded(self):
        sql = "WITH ridership AS (SELECT * FROM source) SELECT * FROM ridership"
        result = extract_table_names(sql)
        assert "ridership" not in result
        assert "source" in result

    def test_multiple_ctes(self):
        sql = """
        WITH
          a AS (SELECT * FROM raw_a),
          b AS (SELECT * FROM raw_b)
        SELECT * FROM a JOIN b ON a.id = b.id
        """
        result = extract_table_names(sql)
        assert "raw_a" in result
        assert "raw_b" in result
        assert "a" not in result
        assert "b" not in result

    def test_subquery(self):
        sql = "SELECT * FROM (SELECT * FROM inner_table) AS subq"
        result = extract_table_names(sql)
        assert "inner_table" in result

    def test_union(self):
        sql = "SELECT * FROM table_a UNION ALL SELECT * FROM table_b"
        result = extract_table_names(sql)
        assert "table_a" in result
        assert "table_b" in result

    def test_malformed_sql_uses_regex_fallback(self):
        sql = "SELECT * FROM weird_table_name"
        result = extract_table_names(sql)
        assert "weird_table_name" in result

    def test_empty_sql(self):
        sql = ""
        with pytest.raises(Exception):
            extract_table_names(sql)


class TestExtractQualifiedTableNames:
    def test_three_part_name(self):
        sql = "SELECT * FROM lake.nyc_ops.service_requests"
        result = extract_qualified_table_names(sql)
        assert "lake.nyc_ops.service_requests" in result

    def test_unqualified_name_excluded(self):
        sql = "SELECT * FROM my_table"
        result = extract_qualified_table_names(sql)
        assert "my_table" not in result

    def test_two_part_name_excluded(self):
        sql = "SELECT * FROM schema.table_name"
        result = extract_qualified_table_names(sql)
        assert len(result) == 0

    def test_mixed_qualified_and_unqualified(self):
        sql = """
        SELECT * FROM lake.nyc_ops.service_requests
        JOIN local_table ON local_table.id = lake.nyc_ops.service_requests.id
        """
        result = extract_qualified_table_names(sql)
        assert "lake.nyc_ops.service_requests" in result
        assert "local_table" not in result

    def test_cte_excluded_from_qualified(self):
        sql = """
        WITH data AS (SELECT * FROM lake.public.source_t)
        SELECT * FROM data
        """
        result = extract_qualified_table_names(sql)
        assert "data" not in result
        assert "lake.public.source_t" in result

    def test_multiple_qualified_names(self):
        sql = """
        SELECT a.*, b.*
        FROM lake.nyc_ops.service_requests a
        JOIN lake.nyc_health.inspections b ON a.id = b.id
        """
        result = extract_qualified_table_names(sql)
        assert "lake.nyc_ops.service_requests" in result
        assert "lake.nyc_health.inspections" in result
