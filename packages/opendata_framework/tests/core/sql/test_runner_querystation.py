import pytest
from datetime import datetime, timezone

from opendata_framework.core.sql.runner_querystation import (
    render_sql,
    _fmt_ts_with_offset,
    _SAFE_PARTITION_KEY,
)


class TestRenderSqlPartitionStartEnd:
    def test_partition_start_date(self):
        sql = "SELECT * FROM t WHERE date >= {{partition_start}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1),
            partition_end=datetime(2024, 2, 1),
        )
        assert result == "SELECT * FROM t WHERE date >= '2024-01-01'"

    def test_partition_end_date(self):
        sql = "SELECT * FROM t WHERE date < {{partition_end}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1),
            partition_end=datetime(2024, 2, 1),
        )
        assert result == "SELECT * FROM t WHERE date < '2024-02-01'"

    def test_both_partition_dates(self):
        sql = "SELECT * FROM t WHERE date >= {{partition_start}} AND date < {{partition_end}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 6, 1),
            partition_end=datetime(2024, 7, 1),
        )
        assert "'2024-06-01'" in result
        assert "'2024-07-01'" in result

    def test_missing_partition_start_raises(self):
        sql = "SELECT * FROM t WHERE date >= {{partition_start}}"
        with pytest.raises(ValueError, match="not time-partitioned"):
            render_sql(sql)

    def test_missing_partition_end_raises(self):
        sql = "SELECT * FROM t WHERE date < {{partition_end}}"
        with pytest.raises(ValueError, match="not time-partitioned"):
            render_sql(sql)


class TestRenderSqlPartitionTimestamps:
    def test_partition_start_ts(self):
        sql = "SELECT * FROM t WHERE ts >= {{partition_start_ts}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            partition_end=datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        assert "'2024-01-01 00:00:00+00:00'" in result

    def test_partition_end_ts(self):
        sql = "WHERE ts < {{partition_end_ts}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1, tzinfo=timezone.utc),
            partition_end=datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        assert "'2024-02-01 00:00:00+00:00'" in result

    def test_ts_with_negative_offset(self):
        sql = "WHERE ts >= {{partition_start_ts}}"
        from datetime import timedelta

        tz = timezone(timedelta(hours=-5))
        result = render_sql(
            sql,
            partition_start=datetime(2024, 6, 15, 12, 30, tzinfo=tz),
            partition_end=datetime(2024, 7, 1, tzinfo=tz),
        )
        assert "'2024-06-15 12:30:00-05:00'" in result

    def test_missing_ts_raises(self):
        sql = "WHERE ts >= {{partition_start_ts}}"
        with pytest.raises(ValueError, match="not time-partitioned"):
            render_sql(sql)


class TestRenderSqlPartitionKey:
    def test_simple_key(self):
        sql = "SELECT * FROM t WHERE category = {{partition_key}}"
        result = render_sql(sql, partition_key="electronics")
        assert result == "SELECT * FROM t WHERE category = 'electronics'"

    def test_key_with_spaces(self):
        sql = "WHERE category = {{partition_key}}"
        result = render_sql(sql, partition_key="new york")
        assert result == "WHERE category = 'new york'"

    def test_key_with_hyphens_and_dots(self):
        sql = "WHERE key = {{partition_key}}"
        result = render_sql(sql, partition_key="2024-Q1.report")
        assert result == "WHERE key = '2024-Q1.report'"

    def test_missing_partition_key_raises(self):
        sql = "SELECT * FROM t WHERE cat = {{partition_key}}"
        with pytest.raises(ValueError, match="no partition context"):
            render_sql(sql)

    def test_injection_with_semicolon_raises(self):
        sql = "SELECT * FROM t WHERE cat = {{partition_key}}"
        with pytest.raises(ValueError, match="characters disallowed"):
            render_sql(sql, partition_key="'; DROP TABLE t;--")

    def test_injection_with_parentheses_raises(self):
        sql = "WHERE cat = {{partition_key}}"
        with pytest.raises(ValueError, match="characters disallowed"):
            render_sql(sql, partition_key="val(1)")


class TestRenderSqlMixedTokens:
    def test_no_tokens_no_change(self):
        sql = "SELECT 1"
        assert render_sql(sql) == "SELECT 1"

    def test_multiple_same_tokens(self):
        sql = "WHERE date >= {{partition_start}} AND date < {{partition_end}}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1),
            partition_end=datetime(2024, 12, 31),
        )
        assert result.count("'2024") == 2

    def test_whitespace_around_tokens(self):
        sql = "WHERE date >= {{ partition_start }} AND date < {{ partition_end }}"
        result = render_sql(
            sql,
            partition_start=datetime(2024, 1, 1),
            partition_end=datetime(2024, 12, 31),
        )
        assert "'2024-01-01'" in result
        assert "'2024-12-31'" in result


class TestFmtTsWithOffset:
    def test_utc(self):
        dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=timezone.utc)
        result = _fmt_ts_with_offset(dt)
        assert result == "2024-01-15 10:30:45+00:00"

    def test_naive_datetime(self):
        dt = datetime(2024, 6, 1, 12, 0, 0)
        result = _fmt_ts_with_offset(dt)
        assert result == "2024-06-01 12:00:00"

    def test_negative_offset(self):
        from datetime import timedelta

        tz = timezone(timedelta(hours=-5))
        dt = datetime(2024, 7, 4, 14, 30, 0, tzinfo=tz)
        result = _fmt_ts_with_offset(dt)
        assert result == "2024-07-04 14:30:00-05:00"


class TestSafePartitionKeyRegex:
    @pytest.mark.parametrize(
        "valid_key",
        [
            "2024",
            "electronics",
            "new york",
            "2024-Q1",
            "category.name",
            "a_b_c",
            "2024-01-01",
        ],
    )
    def test_valid_keys(self, valid_key):
        assert _SAFE_PARTITION_KEY.match(valid_key)

    @pytest.mark.parametrize(
        "invalid_key",
        [
            "'; DROP TABLE",
            "val(1)",
            "a & b",
            'quote"injection',
        ],
    )
    def test_invalid_keys(self, invalid_key):
        assert not _SAFE_PARTITION_KEY.match(invalid_key)
