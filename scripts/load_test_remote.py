#!/usr/bin/env python3
"""Load-test the QueryStation Arrow IPC endpoint.

Probes three failure dimensions:

1. Latency ladder — graduated queries from cheap aggregate to large scan
   against 311 and MTA subway hourly. Measures latency, rows, payload bytes,
   and rows/sec.
2. Sustained throughput — repeats a medium query N times sequentially.
   Reports p50/p95/max latency and total wall time.
3. Concurrency sweep — fires the medium query with C parallel workers
   (default 1, 2, 4, 8). Reports per-request and aggregate throughput.

Bypasses ``RemoteDuckDBWrapper.sql()`` for the actual POST so we can set a
larger timeout (the wrapper hardcodes 30s). Reuses ``QueryStationAuth`` so
every worker shares one cached JWT.

Usage:
    uv run python scripts/load_test_remote.py
    uv run python scripts/load_test_remote.py --phases ladder
    uv run python scripts/load_test_remote.py --phases ladder,sustained
    uv run python scripts/load_test_remote.py --concurrency 1,2,4,8,16
    uv run python scripts/load_test_remote.py --sustained-iters 20 --timeout 300
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import httpx
import pyarrow as pa
from dotenv import load_dotenv
from rich import box
from rich.console import Console
from rich.table import Table

from data_consumers._auth import QueryStationAuth

load_dotenv()
console = Console()


# ── test catalog ────────────────────────────────────────────

@dataclass(frozen=True)
class Query:
    tier: str           # tiny | small | medium | large | huge
    table: str          # short label (311 / mta_hourly)
    label: str          # human-readable description
    sql: str

# 311: 2024–2026 window, 58 columns. Known-safe cols: borough, agency_name,
# complaint_type, year, month, hour, day_of_week.
# MTA hourly: 2020–2026 window, 24 columns. Known-safe cols: borough,
# station_complex, payment_method, fare_class_category, ridership, transfers,
# year, month, hour, day_of_week.

QUERIES: list[Query] = [
    # 311 ladder (lake.nyc_operations.service_requests_311 — 2024-2026, 58 cols)
    Query("tiny",   "311", "count one month",
          "SELECT COUNT(*) AS n FROM lake.nyc_operations.service_requests_311 "
          "WHERE year = 2025 AND month = 6"),
    Query("small",  "311", "borough x year",
          "SELECT borough, year, COUNT(*) AS n "
          "FROM lake.nyc_operations.service_requests_311 "
          "GROUP BY 1, 2 ORDER BY 2, 1"),
    Query("medium", "311", "agency x year x month",
          "SELECT agency_name, year, month, COUNT(*) AS n "
          "FROM lake.nyc_operations.service_requests_311 "
          "GROUP BY 1, 2, 3"),
    Query("large",  "311", "complaint x borough x year x month",
          "SELECT complaint_type, borough, year, month, COUNT(*) AS n "
          "FROM lake.nyc_operations.service_requests_311 "
          "GROUP BY 1, 2, 3, 4"),
    Query("huge",   "311", "raw scan 250k rows",
          "SELECT unique_key, created_date, agency_name, complaint_type, borough "
          "FROM lake.nyc_operations.service_requests_311 "
          "WHERE year = 2025 LIMIT 250000"),

    # MTA subway hourly ladder (lake.nys_transportation.mta_subway_hourly_ridership — 2020-2026, 24 cols)
    Query("tiny",   "mta_hourly", "count one month",
          "SELECT COUNT(*) AS n FROM lake.nys_transportation.mta_subway_hourly_ridership "
          "WHERE year = 2024 AND month = 1"),
    Query("small",  "mta_hourly", "borough x year",
          "SELECT borough, year, ROUND(SUM(ridership), 0) AS riders "
          "FROM lake.nys_transportation.mta_subway_hourly_ridership "
          "GROUP BY 1, 2 ORDER BY 2, 1"),
    Query("medium", "mta_hourly", "station x year",
          "SELECT station_complex, year, ROUND(SUM(ridership), 0) AS riders "
          "FROM lake.nys_transportation.mta_subway_hourly_ridership "
          "GROUP BY 1, 2"),
    Query("large",  "mta_hourly", "station x hour x year",
          "SELECT station_complex, hour, year, ROUND(SUM(ridership), 0) AS riders "
          "FROM lake.nys_transportation.mta_subway_hourly_ridership "
          "GROUP BY 1, 2, 3"),
    Query("huge",   "mta_hourly", "raw scan 500k rows",
          "SELECT transit_timestamp, station_complex, borough, ridership, transfers "
          "FROM lake.nys_transportation.mta_subway_hourly_ridership "
          "WHERE year = 2024 LIMIT 500000"),

    # Cross-schema breadth — one tiny + one medium per table to compare
    # latency profiles across the catalog.
    Query("tiny",   "payroll", "count one fiscal year",
          "SELECT COUNT(*) AS n FROM lake.nyc_finance.city_payroll "
          "WHERE fiscal_year = 2024"),
    Query("medium", "payroll", "agency x fiscal_year aggregate",
          "SELECT agency_name, fiscal_year, COUNT(*) AS workers, "
          "ROUND(SUM(regular_gross_paid), 0) AS gross "
          "FROM lake.nyc_finance.city_payroll GROUP BY 1, 2"),

    Query("tiny",   "checkbook", "count one fiscal year",
          "SELECT COUNT(*) AS n FROM lake.nyc_checkbook.checkbook_spending "
          "WHERE fiscal_year = 2024"),
    Query("medium", "checkbook", "agency x fiscal_year spend",
          "SELECT agency, fiscal_year, COUNT(*) AS txns, "
          "ROUND(SUM(check_amount), 0) AS spend "
          "FROM lake.nyc_checkbook.checkbook_spending GROUP BY 1, 2"),

    Query("tiny",   "restaurants", "count one borough",
          "SELECT COUNT(*) AS n FROM lake.nyc_health.restaurant_inspections "
          "WHERE boro = 'Manhattan'"),
    Query("medium", "restaurants", "cuisine x grade aggregate",
          "SELECT cuisine_description, grade, COUNT(*) AS n "
          "FROM lake.nyc_health.restaurant_inspections "
          "WHERE grade IS NOT NULL GROUP BY 1, 2"),

    Query("tiny",   "arrests", "count one year",
          "SELECT COUNT(*) AS n FROM lake.nyc_public_safety.nypd_arrests "
          "WHERE year = 2024"),
    Query("medium", "arrests", "ofns_desc x borough x year",
          "SELECT ofns_desc, arrest_boro, year, COUNT(*) AS n "
          "FROM lake.nyc_public_safety.nypd_arrests GROUP BY 1, 2, 3"),

    Query("tiny",   "capital", "count one fiscal year",
          "SELECT COUNT(*) AS n FROM lake.nyc_finance.capital_budget "
          "WHERE fiscal_year = '2024'"),
    Query("medium", "capital", "sponsor x fiscal_year planned",
          "SELECT sponsor, fiscal_year, COUNT(*) AS line_items, "
          "ROUND(SUM(award), 0) AS total_award "
          "FROM lake.nyc_finance.capital_budget GROUP BY 1, 2"),

    Query("tiny",   "courts", "count one year",
          "SELECT COUNT(*) AS n FROM lake.nys_courts.pretrial_release "
          "WHERE arrest_year = '2024'"),
    Query("medium", "courts", "court x arrest_year",
          "SELECT court_name, arrest_year, COUNT(*) AS n "
          "FROM lake.nys_courts.pretrial_release GROUP BY 1, 2"),
]

# Sustained / concurrency use only the original 311 + MTA medium queries
# to keep total runtime bounded.
SUSTAINED_QUERIES: list[Query] = [
    q for q in QUERIES
    if q.tier == "medium" and q.table in ("311", "mta_hourly")
]

# Scan ladder — graduated LIMIT to probe wire bandwidth ceiling and where
# the endpoint starts choking on response size. Always uses MTA hourly
# (the largest underlying table).
def _scan_query(rows: int) -> Query:
    label = f"scan {rows:,} rows"
    sql = (
        "SELECT transit_timestamp, station_complex, borough, "
        "ridership, transfers "
        "FROM lake.nys_transportation.mta_subway_hourly_ridership "
        f"LIMIT {rows}"
    )
    tier = (
        "tiny" if rows <= 10_000
        else "small" if rows <= 100_000
        else "medium" if rows <= 500_000
        else "large" if rows <= 2_000_000
        else "huge"
    )
    return Query(tier, "mta_hourly", label, sql)

SCAN_LADDER: list[Query] = [
    _scan_query(n) for n in (10_000, 100_000, 500_000, 1_000_000, 2_000_000, 5_000_000)
]


# ── transport ───────────────────────────────────────────────

@dataclass
class QueryResult:
    query: Query
    ok: bool
    latency_s: float
    rows: int = 0
    bytes_: int = 0
    error: str = ""

    @property
    def mb(self) -> float:
        return self.bytes_ / (1024 * 1024)

    @property
    def rows_per_s(self) -> float:
        return self.rows / self.latency_s if self.latency_s > 0 else 0.0

    @property
    def mb_per_s(self) -> float:
        return self.mb / self.latency_s if self.latency_s > 0 else 0.0


def run_one(
    query: Query,
    auth: QueryStationAuth,
    timeout: float,
    no_cache: bool = False,
) -> QueryResult:
    """Send one Arrow IPC query and time end-to-end (network + parse).

    When ``no_cache`` is True, appends a unique nonce comment so the SQL
    string differs every call — defeats any server-side result cache keyed
    on the query text. Trailing form is required because the QueryStation
    endpoint rejects SQL that *begins* with a comment (HTTP 403).
    """
    token = auth.get_token()
    url = auth.remote_url
    sql = query.sql
    if no_cache:
        sql = f"{sql} -- nonce: {uuid.uuid4().hex}"
    started = time.perf_counter()
    try:
        r = httpx.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"type": "arrow", "sql": sql},
            timeout=timeout,
        )
        if r.status_code == 401:
            auth.force_refresh()
            token = auth.get_token()
            r = httpx.post(
                url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={"type": "arrow", "sql": sql},
                timeout=timeout,
            )
        r.raise_for_status()
        payload = r.content
        table = pa.ipc.open_stream(payload).read_all()
        elapsed = time.perf_counter() - started
        return QueryResult(query, True, elapsed, table.num_rows, len(payload))
    except httpx.TimeoutException:
        return QueryResult(query, False, time.perf_counter() - started,
                           error=f"timeout after {timeout}s")
    except httpx.HTTPStatusError as exc:
        return QueryResult(query, False, time.perf_counter() - started,
                           error=f"HTTP {exc.response.status_code}")
    except Exception as exc:
        return QueryResult(query, False, time.perf_counter() - started,
                           error=f"{type(exc).__name__}: {exc}"[:80])


# ── phases ──────────────────────────────────────────────────

def phase_ladder(
    auth: QueryStationAuth,
    timeout: float,
    no_cache: bool = False,
) -> list[QueryResult]:
    label = "Phase 1: Latency ladder" + (" [no-cache]" if no_cache else "")
    console.print(f"\n[bold bright_yellow]── {label} ──[/]")
    results: list[QueryResult] = []
    for q in QUERIES:
        console.print(f"  [dim]{q.table:12s} {q.tier:6s}[/] {q.label} ...", end=" ")
        res = run_one(q, auth, timeout, no_cache=no_cache)
        if res.ok:
            console.print(f"[green]{res.latency_s:6.2f}s[/] "
                          f"{res.rows:>9,} rows  {res.mb:6.2f} MB")
        else:
            console.print(f"[red]FAIL[/] {res.error}")
        results.append(res)
    return results


def phase_scan_ladder(
    auth: QueryStationAuth,
    timeout: float,
    no_cache: bool = False,
) -> list[QueryResult]:
    label = "Phase 4: Scan ladder (payload size)" + (" [no-cache]" if no_cache else "")
    console.print(f"\n[bold bright_yellow]── {label} ──[/]")
    results: list[QueryResult] = []
    for q in SCAN_LADDER:
        console.print(f"  [dim]{q.label}[/] ...", end=" ")
        res = run_one(q, auth, timeout, no_cache=no_cache)
        if res.ok:
            console.print(f"[green]{res.latency_s:6.2f}s[/] "
                          f"{res.rows:>9,} rows  {res.mb:7.2f} MB  "
                          f"{res.mb_per_s:5.2f} MB/s")
        else:
            console.print(f"[red]FAIL[/] {res.error}")
        results.append(res)
    return results


def phase_sustained(
    auth: QueryStationAuth,
    iters: int,
    timeout: float,
    no_cache: bool = False,
) -> dict[str, list[QueryResult]]:
    label = f"Phase 2: Sustained throughput ({iters} iters)" + (" [no-cache]" if no_cache else "")
    console.print(f"\n[bold bright_yellow]── {label} ──[/]")
    by_table: dict[str, list[QueryResult]] = {}
    for q in SUSTAINED_QUERIES:
        console.print(f"\n  [cyan]{q.table}[/] · {q.label}")
        runs: list[QueryResult] = []
        for i in range(iters):
            res = run_one(q, auth, timeout, no_cache=no_cache)
            status = f"[green]{res.latency_s:.2f}s[/]" if res.ok else f"[red]FAIL ({res.error})[/]"
            console.print(f"    iter {i+1:>2}/{iters}  {status}")
            runs.append(res)
        by_table[q.table] = runs
    return by_table


def phase_concurrency(
    auth: QueryStationAuth,
    levels: list[int],
    timeout: float,
    no_cache: bool = False,
) -> dict[tuple[str, int], list[QueryResult]]:
    label = f"Phase 3: Concurrency sweep ({levels})" + (" [no-cache]" if no_cache else "")
    console.print(f"\n[bold bright_yellow]── {label} ──[/]")
    by_run: dict[tuple[str, int], list[QueryResult]] = {}
    for q in SUSTAINED_QUERIES:
        console.print(f"\n  [cyan]{q.table}[/] · {q.label}")
        for c in levels:
            wall_started = time.perf_counter()
            with ThreadPoolExecutor(max_workers=c) as pool:
                futs = [pool.submit(run_one, q, auth, timeout, no_cache) for _ in range(c)]
                runs = [f.result() for f in as_completed(futs)]
            wall = time.perf_counter() - wall_started
            ok = [r for r in runs if r.ok]
            failed = len(runs) - len(ok)
            if ok:
                lats = [r.latency_s for r in ok]
                console.print(
                    f"    c={c:>2}  wall={wall:5.2f}s  "
                    f"ok={len(ok)}/{c}  "
                    f"per-req min/med/max = {min(lats):.2f}/{statistics.median(lats):.2f}/{max(lats):.2f}s  "
                    f"agg={sum(r.rows for r in ok) / wall:>10,.0f} rows/s"
                )
            else:
                console.print(f"    c={c:>2}  [red]all {failed} requests failed[/]")
            by_run[(q.table, c)] = runs
    return by_run


# ── reporting ───────────────────────────────────────────────

def report_ladder(results: list[QueryResult]) -> None:
    t = Table(title="Latency ladder", title_style="bold bright_yellow",
              header_style="bold bright_white", box=box.ROUNDED,
              border_style="bright_black", show_lines=False)
    for col in ("table", "tier", "label", "latency", "rows", "MB", "rows/s", "MB/s", "status"):
        t.add_column(col, style="bright_cyan", justify="left", overflow="ellipsis")
    for r in results:
        if r.ok:
            t.add_row(r.query.table, r.query.tier, r.query.label,
                      f"{r.latency_s:.2f}s", f"{r.rows:,}", f"{r.mb:.2f}",
                      f"{r.rows_per_s:,.0f}", f"{r.mb_per_s:.2f}", "[green]OK[/]")
        else:
            t.add_row(r.query.table, r.query.tier, r.query.label,
                      f"{r.latency_s:.2f}s", "-", "-", "-", "-",
                      f"[red]{r.error}[/]")
    console.print()
    console.print(t)


def report_sustained(by_table: dict[str, list[QueryResult]]) -> None:
    t = Table(title="Sustained throughput", title_style="bold bright_yellow",
              header_style="bold bright_white", box=box.ROUNDED,
              border_style="bright_black")
    for col in ("table", "iters", "ok", "p50", "p95", "max", "total wall", "status"):
        t.add_column(col, style="bright_cyan")
    for table, runs in by_table.items():
        ok = [r.latency_s for r in runs if r.ok]
        failed = len(runs) - len(ok)
        if ok:
            p50 = statistics.median(ok)
            p95 = sorted(ok)[max(0, int(len(ok) * 0.95) - 1)] if len(ok) > 1 else ok[0]
            t.add_row(table, str(len(runs)), f"{len(ok)}/{len(runs)}",
                      f"{p50:.2f}s", f"{p95:.2f}s", f"{max(ok):.2f}s",
                      f"{sum(ok):.2f}s",
                      "[green]OK[/]" if failed == 0 else f"[yellow]{failed} failed[/]")
        else:
            t.add_row(table, str(len(runs)), "0", "-", "-", "-", "-", "[red]all failed[/]")
    console.print()
    console.print(t)


def report_concurrency(by_run: dict[tuple[str, int], list[QueryResult]]) -> None:
    t = Table(title="Concurrency sweep", title_style="bold bright_yellow",
              header_style="bold bright_white", box=box.ROUNDED,
              border_style="bright_black")
    for col in ("table", "concurrency", "ok", "min lat", "med lat", "max lat",
                "agg rows/s", "agg MB/s"):
        t.add_column(col, style="bright_cyan")
    for (table, c), runs in by_run.items():
        ok = [r for r in runs if r.ok]
        if not ok:
            t.add_row(table, str(c), f"0/{c}", "-", "-", "-", "-", "-")
            continue
        lats = [r.latency_s for r in ok]
        # Aggregate throughput approximated as sum(rows) / max(latency) — an
        # honest lower bound when futures complete near-simultaneously.
        wall = max(lats)
        agg_rows = sum(r.rows for r in ok) / wall if wall > 0 else 0.0
        agg_mb = sum(r.mb for r in ok) / wall if wall > 0 else 0.0
        t.add_row(table, str(c), f"{len(ok)}/{len(runs)}",
                  f"{min(lats):.2f}s", f"{statistics.median(lats):.2f}s",
                  f"{max(lats):.2f}s",
                  f"{agg_rows:,.0f}", f"{agg_mb:.2f}")
    console.print()
    console.print(t)


# ── main ────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--phases", default="ladder,sustained,concurrency",
                        help="Comma-separated subset of: ladder, sustained, concurrency, scan_ladder")
    parser.add_argument("--timeout", type=float, default=180.0,
                        help="Per-request timeout in seconds (default 180)")
    parser.add_argument("--sustained-iters", type=int, default=10,
                        help="Iterations per table in sustained phase (default 10)")
    parser.add_argument("--concurrency", default="1,2,4,8",
                        help="Comma-separated worker counts for concurrency phase")
    parser.add_argument("--no-cache", action="store_true",
                        help="Append a unique nonce to every SQL to defeat result caching")
    args = parser.parse_args()

    phases = {p.strip() for p in args.phases.split(",") if p.strip()}
    levels = [int(x) for x in args.concurrency.split(",") if x.strip()]

    try:
        auth = QueryStationAuth()
        # Force one exchange up front so per-query timing isn't polluted.
        _ = auth.get_token()
    except Exception as exc:
        console.print(f"[red]Auth failed:[/] {exc}")
        return 1

    console.print(f"[dim]endpoint: {auth.remote_url}[/]")
    console.print(f"[dim]timeout: {args.timeout}s · phases: {sorted(phases)}"
                  f"{' · no-cache' if args.no_cache else ''}[/]")

    ladder_results: list[QueryResult] = []
    scan_results: list[QueryResult] = []
    sustained_results: dict[str, list[QueryResult]] = {}
    concurrency_results: dict[tuple[str, int], list[QueryResult]] = {}

    if "ladder" in phases:
        ladder_results = phase_ladder(auth, args.timeout, args.no_cache)
    if "scan_ladder" in phases:
        scan_results = phase_scan_ladder(auth, args.timeout, args.no_cache)
    if "sustained" in phases:
        sustained_results = phase_sustained(auth, args.sustained_iters,
                                            args.timeout, args.no_cache)
    if "concurrency" in phases:
        concurrency_results = phase_concurrency(auth, levels, args.timeout,
                                                args.no_cache)

    console.print("\n[bold bright_yellow]══ Summary ══[/]")
    if ladder_results:
        report_ladder(ladder_results)
    if scan_results:
        report_ladder(scan_results)
    if sustained_results:
        report_sustained(sustained_results)
    if concurrency_results:
        report_concurrency(concurrency_results)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
