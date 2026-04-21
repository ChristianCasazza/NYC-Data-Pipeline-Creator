/*---
name: mta_ridership_yearly
source: querystation
description: >
  Yearly per-mode MTA ridership aggregates pulled from QueryStation.
  One partition = one remote query = one Hive parquet shard.
group: querystation__transportation
partitions:
  type: yearly
  start: "2020"
  end_offset: 1
tags:
  domain: transportation
  geographic_scope: nys
  stage: analytics
---*/

SELECT
    extract(year FROM date)::INT             AS year,
    mode,
    count(*)                                 AS days_reported,
    round(sum(count), 0)                     AS total_riders,
    round(avg(count), 1)                     AS avg_daily_riders,
    round(max(count), 0)                     AS peak_daily_riders,
    min(date)                                AS first_date,
    max(date)                                AS last_date
FROM lake.nys_transportation.mta_daily_ridership
WHERE date >= {{partition_start}}
  AND date <  {{partition_end}}
GROUP BY 1, 2
ORDER BY total_riders DESC
