#!/usr/bin/env python3
"""Run 15 ad-hoc queries across 5 different remote tables. One-shot
exploration via QueryStation. Uses partition pruning where the table
exposes a `year` column."""
from __future__ import annotations

import time
from dataclasses import dataclass

import polars as pl
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.table import Table

from data_consumers import RemoteDuckDBWrapper

load_dotenv()
console = Console()


@dataclass(frozen=True)
class Q:
    table: str
    title: str
    sql: str


QUERIES: list[Q] = [
    # ── city_payroll ──────────────────────────────────────────
    Q("payroll", "Top 10 agencies by FY2024 gross pay",
      """
      SELECT agency_name,
             COUNT(*) AS workers,
             ROUND(SUM(regular_gross_paid + total_ot_paid + total_other_pay), 0) AS total_pay
      FROM lake.nyc_finance.city_payroll
      WHERE fiscal_year = 2024
      GROUP BY 1
      ORDER BY total_pay DESC
      LIMIT 10
      """),
    Q("payroll", "Median base salary by pay basis (FY2024, salaried only)",
      """
      SELECT pay_basis,
             COUNT(*) AS workers,
             ROUND(median(base_salary), 0) AS median_base,
             ROUND(quantile_cont(base_salary, 0.95), 0) AS p95_base,
             ROUND(MAX(base_salary), 0) AS max_base
      FROM lake.nyc_finance.city_payroll
      WHERE fiscal_year = 2024 AND base_salary > 0
      GROUP BY 1
      ORDER BY median_base DESC
      """),
    Q("payroll", "Top 10 agencies by FY2024 OT-to-base ratio",
      """
      SELECT agency_name,
             ROUND(SUM(total_ot_paid), 0) AS ot_paid,
             ROUND(SUM(regular_gross_paid), 0) AS regular_paid,
             ROUND(SUM(total_ot_paid) / NULLIF(SUM(regular_gross_paid), 0), 3) AS ot_ratio
      FROM lake.nyc_finance.city_payroll
      WHERE fiscal_year = 2024
      GROUP BY 1
      HAVING SUM(regular_gross_paid) > 1e7
      ORDER BY ot_ratio DESC
      LIMIT 10
      """),

    # ── restaurant_inspections ───────────────────────────────
    Q("restaurants", "Grade distribution for top 15 cuisines",
      """
      SELECT cuisine_description,
             COUNT(*) AS inspections,
             ROUND(100.0 * SUM(CASE WHEN grade = 'A' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_A,
             ROUND(100.0 * SUM(CASE WHEN grade = 'B' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_B,
             ROUND(100.0 * SUM(CASE WHEN grade = 'C' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_C
      FROM lake.nyc_health.restaurant_inspections
      WHERE grade IN ('A','B','C')
      GROUP BY 1
      HAVING COUNT(*) > 5000
      ORDER BY inspections DESC
      LIMIT 15
      """),
    Q("restaurants", "Most common critical violations",
      """
      SELECT violation_description,
             COUNT(*) AS occurrences
      FROM lake.nyc_health.restaurant_inspections
      WHERE critical_flag = 'Critical' AND violation_description IS NOT NULL
      GROUP BY 1
      ORDER BY occurrences DESC
      LIMIT 10
      """),
    Q("restaurants", "Worst-scoring Manhattan inspections (2025)",
      """
      SELECT dba, cuisine_description, score, grade, inspection_date
      FROM lake.nyc_health.restaurant_inspections
      WHERE year = 2025 AND boro = 'Manhattan' AND score IS NOT NULL
      ORDER BY score DESC
      LIMIT 10
      """),

    # ── shootings_by_victim ──────────────────────────────────
    Q("shootings", "Shootings by borough × year (2020-2024)",
      """
      SELECT year, boro, COUNT(*) AS victims
      FROM lake.nyc_public_safety.shootings_by_victim
      WHERE year BETWEEN 2020 AND 2024
      GROUP BY 1, 2
      ORDER BY 1, 3 DESC
      """),
    Q("shootings", "Murder rate by victim age group (all years)",
      """
      SELECT victim_age_group,
             COUNT(*) AS shootings,
             SUM(CASE WHEN stat_murder_flg = 'true' THEN 1 ELSE 0 END) AS murders,
             ROUND(100.0 * SUM(CASE WHEN stat_murder_flg = 'true' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_fatal
      FROM lake.nyc_public_safety.shootings_by_victim
      GROUP BY 1
      ORDER BY shootings DESC
      """),
    Q("shootings", "Top 10 precincts by 2024 victim count",
      """
      SELECT precinct, boro,
             COUNT(*) AS victims,
             SUM(CASE WHEN stat_murder_flg = 'true' THEN 1 ELSE 0 END) AS murders
      FROM lake.nyc_public_safety.shootings_by_victim
      WHERE year = 2024
      GROUP BY 1, 2
      ORDER BY victims DESC
      LIMIT 10
      """),

    # ── housing_connect ──────────────────────────────────────
    Q("housing", "Total units by AMI band (citywide)",
      """
      SELECT
        ROUND(SUM(applied_income_ami_ext_low), 0)  AS units_ext_low,
        ROUND(SUM(applied_income_ami_very_low), 0) AS units_very_low,
        ROUND(SUM(applied_income_ami_low), 0)      AS units_low,
        ROUND(SUM(applied_income_ami_moderate), 0) AS units_moderate,
        ROUND(SUM(applied_income_ami_middle), 0)   AS units_middle,
        ROUND(SUM(applied_income_ami_above), 0)    AS units_above
      FROM lake.nyc_housing.housing_connect
      """),
    Q("housing", "Top 10 lotteries by unit count",
      """
      SELECT lottery_name, borough, unit_count,
             unit_distribution_studio AS studios,
             unit_distribution_1bed   AS one_br,
             unit_distribution_2bed   AS two_br,
             unit_distribution_3bed   AS three_br,
             lottery_status
      FROM lake.nyc_housing.housing_connect
      WHERE unit_count IS NOT NULL
      ORDER BY unit_count DESC
      LIMIT 10
      """),
    Q("housing", "Avg lottery duration & total units by borough",
      """
      SELECT borough,
             COUNT(*) AS lotteries,
             ROUND(AVG(lottery_duration_days), 0) AS avg_duration_days,
             SUM(unit_count) AS total_units
      FROM lake.nyc_housing.housing_connect
      WHERE borough IS NOT NULL
      GROUP BY 1
      ORDER BY total_units DESC
      """),

    # ── air_quality ──────────────────────────────────────────
    Q("air_quality", "All indicators with measurement counts",
      """
      SELECT name AS indicator, measure, COUNT(*) AS readings,
             ROUND(MIN(data_value), 2) AS min_val,
             ROUND(MAX(data_value), 2) AS max_val
      FROM lake.nyc_environment.air_quality
      WHERE data_value IS NOT NULL
      GROUP BY 1, 2
      ORDER BY readings DESC
      LIMIT 15
      """),
    Q("air_quality", "Worst neighborhoods for PM2.5 (most recent annual avg)",
      """
      SELECT geo_place_name, year,
             ROUND(AVG(data_value), 2) AS avg_pm25
      FROM lake.nyc_environment.air_quality
      WHERE name = 'Fine particles (PM 2.5)'
        AND year >= 2020
        AND geo_type_name LIKE '%Neighborhood%'
      GROUP BY 1, 2
      ORDER BY avg_pm25 DESC
      LIMIT 10
      """),
    Q("air_quality", "Citywide NO2 yearly trend",
      """
      SELECT year,
             ROUND(AVG(data_value), 2) AS avg_no2,
             COUNT(DISTINCT geo_place_name) AS sites
      FROM lake.nyc_environment.air_quality
      WHERE name = 'Nitrogen dioxide (NO2)' AND year IS NOT NULL
      GROUP BY 1
      ORDER BY 1
      """),
]


def render(df: pl.DataFrame, title: str, latency: float) -> None:
    t = Table(
        title=f"{title}  [dim]({df.height} rows · {latency:.2f}s)[/]",
        title_style="bold bright_yellow",
        header_style="bold bright_white",
        box=box.SIMPLE,
        border_style="bright_black",
        show_lines=False,
    )
    for col in df.columns:
        t.add_column(col, style="bright_cyan", overflow="ellipsis", max_width=40)
    for row in df.iter_rows():
        t.add_row(*[str(v) if v is not None else "" for v in row])
    console.print(t)


def main() -> int:
    db = RemoteDuckDBWrapper()
    total = 0.0
    failures = 0

    by_table: dict[str, list[Q]] = {}
    for q in QUERIES:
        by_table.setdefault(q.table, []).append(q)

    for table, qs in by_table.items():
        console.print(f"\n[bold bright_yellow]══ {table} ══[/]")
        for q in qs:
            try:
                t0 = time.perf_counter()
                df = db.sql(q.sql)
                latency = time.perf_counter() - t0
                total += latency
                render(df, q.title, latency)
            except Exception as exc:
                failures += 1
                console.print(f"[red]✗ {q.title}[/]  {type(exc).__name__}: {exc}")

    console.print(f"\n[bold]Total: {len(QUERIES)} queries · {total:.1f}s · "
                  f"{failures} failed[/]")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
