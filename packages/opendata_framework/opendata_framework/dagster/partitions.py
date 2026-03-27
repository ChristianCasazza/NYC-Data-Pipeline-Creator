from dagster import TimeWindowPartitionsDefinition, MonthlyPartitionsDefinition


def yearly_partitions(
    start: str,
    end: str | None = None,
    end_offset: int = 0,
    tz: str = "America/New_York",
) -> TimeWindowPartitionsDefinition:
    return TimeWindowPartitionsDefinition(
        start=start,
        end=end,
        fmt="%Y",
        cron_schedule="0 0 1 1 *",
        timezone=tz,
        end_offset=end_offset,
    )


def monthly_partitions(
    start_date: str,
    end_date: str | None = None,
    end_offset: int | None = None,
    tz: str = "America/New_York",
) -> MonthlyPartitionsDefinition:
    kwargs = {"start_date": start_date, "timezone": tz}
    if end_date is not None:
        kwargs["end_date"] = end_date
    if end_offset is not None:
        kwargs["end_offset"] = end_offset
    return MonthlyPartitionsDefinition(**kwargs)
