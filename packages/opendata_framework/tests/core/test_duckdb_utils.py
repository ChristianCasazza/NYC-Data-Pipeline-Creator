import pytest
import duckdb
import polars as pl

from opendata_framework.core.duckdb_utils import (
    connect,
    session,
    query_df,
    read_parquet_via_duckdb,
    ensure_view_from_warehouse,
)


class TestConnect:
    def test_connect_without_warehouse(self):
        con = connect(warehouse_path=None, attach_warehouse=False)
        assert con is not None
        result = con.execute("SELECT 1 AS x").fetchone()
        assert result[0] == 1
        con.close()

    def test_connect_requires_warehouse_path(self):
        with pytest.raises(ValueError, match="warehouse_path is required"):
            connect(warehouse_path=None, attach_warehouse=True)

    def test_connect_creates_memory_db(self):
        con = connect(warehouse_path=None, attach_warehouse=False)
        result = con.execute("SELECT current_database()").fetchone()
        assert result is not None
        con.close()


class TestSession:
    def test_session_yields_connection(self):
        with session(attach_wh=False) as con:
            result = con.execute("SELECT 42 AS answer").fetchone()
            assert result[0] == 42

    def test_session_closes_connection(self):
        with session(attach_wh=False) as con:
            pass
        with pytest.raises(Exception):
            con.execute("SELECT 1")


class TestQueryDF:
    def test_basic_query(self):
        df = query_df("SELECT 1 AS x, 'hello' AS y")
        assert isinstance(df, pl.DataFrame)
        assert df.height == 1
        assert df["x"][0] == 1
        assert df["y"][0] == "hello"

    def test_with_params(self):
        df = query_df("SELECT $1 AS x", params=(42,))
        assert df["x"][0] == 42


class TestReadParquetViaDuckDB:
    def test_read_parquet_file(self, tmp_path):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        path = tmp_path / "test.parquet"
        df.write_parquet(str(path))

        result = read_parquet_via_duckdb(str(path))
        assert result.height == 3
        assert set(result.columns) == {"a", "b"}

    def test_read_with_hive_partitioning(self, tmp_path):
        df = pl.DataFrame({"val": [10, 20], "year": [2023, 2024]})
        (tmp_path / "year=2023").mkdir()
        (tmp_path / "year=2024").mkdir()
        df.filter(pl.col("year") == 2023).drop("year").write_parquet(
            str(tmp_path / "year=2023" / "data.parquet")
        )
        df.filter(pl.col("year") == 2024).drop("year").write_parquet(
            str(tmp_path / "year=2024" / "data.parquet")
        )

        result = read_parquet_via_duckdb(
            str(tmp_path / "**" / "*.parquet"), hive_partitioning=True
        )
        assert result.height == 2


class TestEnsureViewFromWarehouse:
    def test_ensure_view_creates_empty_when_missing(self):
        con = duckdb.connect(":memory:")
        from duckdb import CatalogException

        ensure_view_from_warehouse(con, "my_empty_view", source_name="nonexistent_tbl")
        result = con.execute("SELECT * FROM my_empty_view").fetchall()
        assert result == []
        con.close()
