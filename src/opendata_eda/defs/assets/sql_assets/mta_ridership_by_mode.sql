/*---
name: mta_ridership_by_mode
source: querystation
description: >
  Per-mode MTA ridership totals across all available years, pulled from
  QueryStation's lake.nys_transportation.mta_daily_ridership.
  Unpartitioned — one remote query, one parquet file.
group: querystation__transportation
tags:
  domain: transportation
  geographic_scope: nys
  stage: analytics
  source: querystation
---*/

SELECT
    mode,
    count(*)                             AS days_reported,
    round(sum(count), 0)                 AS total_riders,
    round(avg(count), 1)                 AS avg_daily_riders,
    round(max(count), 0)                 AS peak_daily_riders,
    min(date)                            AS first_date,
    max(date)                            AS last_date
FROM lake.nys_transportation.mta_daily_ridership
GROUP BY 1
ORDER BY total_riders DESC
