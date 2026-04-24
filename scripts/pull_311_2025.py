#!/usr/bin/env python3
"""One-shot: pull all 2025 NYC 311 service requests as a single Parquet.

Pulls in monthly chunks via direct httpx (rather than the wrapper's 30s
timeout) so each request stays bounded and partition pruning narrows the
scan. Concatenates and writes to data/exports/nyc_311_2025.parquet.

Usage:
    uv run python scripts/pull_311_2025.py
    uv run python scripts/pull_311_2025.py --out data/exports/foo.parquet
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

import httpx
import polars as pl
import pyarrow as pa
from dotenv import load_dotenv

from data_consumers._auth import QueryStationAuth

load_dotenv()


def fetch_month(auth: QueryStationAuth, month: int, timeout: float) -> tuple[pl.DataFrame, int, float]:
    sql = (
        "SELECT * FROM lake.nyc_operations.service_requests_311 "
        f"WHERE year = 2025 AND month = {month}"
    )
    t0 = time.perf_counter()
    r = httpx.post(
        auth.remote_url,
        headers={
            "Authorization": f"Bearer {auth.get_token()}",
            "Content-Type": "application/json",
        },
        json={"type": "arrow", "sql": sql},
        timeout=timeout,
    )
    r.raise_for_status()
    table = pa.ipc.open_stream(r.content).read_all()
    df = pl.from_arrow(table)
    return df, len(r.content), time.perf_counter() - t0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", default="data/exports/nyc_311_2025.parquet",
                        help="Output Parquet path")
    parser.add_argument("--timeout", type=float, default=120.0,
                        help="Per-month HTTP timeout (default 120s)")
    args = parser.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    auth = QueryStationAuth()
    print(f"endpoint: {auth.remote_url}")
    print(f"target:   {out}\n")

    frames: list[pl.DataFrame] = []
    total_bytes = 0
    total_started = time.perf_counter()

    for month in range(1, 13):
        try:
            df, payload_bytes, elapsed = fetch_month(auth, month, args.timeout)
        except httpx.HTTPStatusError as exc:
            print(f"month={month:02d}  FAIL HTTP {exc.response.status_code}")
            return 1
        except Exception as exc:
            print(f"month={month:02d}  FAIL {type(exc).__name__}: {exc}")
            return 1

        frames.append(df)
        total_bytes += payload_bytes
        print(f"month={month:02d}  {df.height:>8,} rows  "
              f"{payload_bytes / 1e6:6.2f} MB wire  {elapsed:5.1f}s")

    full = pl.concat(frames, how="vertical_relaxed")
    pull_elapsed = time.perf_counter() - total_started

    print(f"\nconcat: {full.height:,} rows x {full.width} cols")
    print(f"writing {out}...")
    write_started = time.perf_counter()
    full.write_parquet(out, compression="zstd")
    write_elapsed = time.perf_counter() - write_started
    on_disk = out.stat().st_size

    print()
    print(f"  source rows:  {full.height:,}")
    print(f"  columns:      {full.width}")
    print(f"  wire total:   {total_bytes / 1e6:.1f} MB across 12 monthly requests")
    print(f"  pull time:    {pull_elapsed:.1f}s ({total_bytes / 1e6 / pull_elapsed:.1f} MB/s avg)")
    print(f"  write time:   {write_elapsed:.1f}s")
    print(f"  on disk:      {on_disk / 1e6:.1f} MB (zstd parquet)")
    print(f"  compression:  {total_bytes / on_disk:.1f}x vs Arrow IPC wire payload")
    print(f"  output:       {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
