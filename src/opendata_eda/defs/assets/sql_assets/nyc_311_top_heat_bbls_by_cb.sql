/*---
name: nyc_311_top_heat_bbls_by_cb
source: querystation
description: >
  Top 10 BBLs per community board by 311 HEAT/HOT WATER and Non-Residential
  Heat complaints. Scoped to the 59 canonical NYC community boards (CBs 1-18
  per borough). Excludes joint-interest areas (JFK, LGA, Central Park, etc.)
  and records with unresolved community board. Uses cb_number as the
  join-ready integer key; borough is derived from cb_number // 100 to
  eliminate upstream borough-mislabel errors.
group: querystation__operations
tags:
  domain: operations
  geographic_scope: nyc
  stage: analytics
---*/

WITH heat_by_bbl AS (
    SELECT
        cb_number,
        CASE cb_number // 100
            WHEN 1 THEN 'MANHATTAN'
            WHEN 2 THEN 'BRONX'
            WHEN 3 THEN 'BROOKLYN'
            WHEN 4 THEN 'QUEENS'
            WHEN 5 THEN 'STATEN ISLAND'
        END                                                                               AS borough,
        cb_number % 100                                                                   AS cb_in_borough,
        bbl,
        count(*)                                                                          AS heat_complaints,
        sum(CASE WHEN complaint_type = 'HEAT/HOT WATER'       THEN 1 ELSE 0 END)::BIGINT  AS residential_heat,
        sum(CASE WHEN complaint_type = 'Non-Residential Heat' THEN 1 ELSE 0 END)::BIGINT  AS non_residential_heat,
        min(created_date::DATE)                                                           AS first_complaint,
        max(created_date::DATE)                                                           AS last_complaint,
        count(DISTINCT date_trunc('month', created_date))                                 AS months_with_complaints
    FROM lake.nyc_operations.service_requests_311
    WHERE complaint_type IN ('HEAT/HOT WATER', 'Non-Residential Heat')
      AND bbl IS NOT NULL
      AND cb_number IS NOT NULL
      AND cb_number BETWEEN 101 AND 599
      AND cb_number % 100 BETWEEN 1 AND 18
    GROUP BY cb_number, bbl
),
ranked AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY cb_number
            ORDER BY heat_complaints DESC, bbl
        ) AS rank_in_cb
    FROM heat_by_bbl
)
SELECT
    cb_number,
    borough,
    cb_in_borough,
    rank_in_cb,
    bbl,
    heat_complaints,
    residential_heat,
    non_residential_heat,
    first_complaint,
    last_complaint,
    months_with_complaints
FROM ranked
WHERE rank_in_cb <= 10
ORDER BY cb_number, rank_in_cb
