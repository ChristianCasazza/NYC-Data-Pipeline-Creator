/*---
name: transit_vs_air_quality
description: >
  Per-year cross-reference of MTA ridership vs. NYC air quality.
  Highlights the 2020 pandemic drop in both transit and pollution,
  and the divergent recoveries. Joins the remote-pulled Parquet
  outputs of mta_ridership_yearly and nyc_air_quality_annual.
deps:
  - mta_ridership_yearly
  - nyc_air_quality_annual
group: querystation__analytics
tags:
  domain: environment
  geographic_scope: nyc
  stage: analytics
---*/

WITH ridership AS (
    SELECT
        year,
        round(sum(total_riders) / 1e6, 1)                                AS total_riders_millions,
        round(sum(CASE WHEN mode = 'Subway' THEN total_riders END) / 1e6, 1) AS subway_riders_millions,
        round(sum(CASE WHEN mode = 'Bus'    THEN total_riders END) / 1e6, 1) AS bus_riders_millions
    FROM mta_ridership_yearly
    GROUP BY 1
),
air AS (
    SELECT
        year,
        max(CASE WHEN indicator = 'Nitrogen dioxide (NO2)'  THEN avg_value END) AS no2_ppb,
        max(CASE WHEN indicator = 'Fine particles (PM 2.5)' THEN avg_value END) AS pm25_mcg_per_m3,
        max(CASE WHEN indicator = 'Ozone (O3)'              THEN avg_value END) AS o3_ppb
    FROM nyc_air_quality_annual
    GROUP BY 1
)
SELECT
    r.year,
    r.total_riders_millions,
    r.subway_riders_millions,
    r.bus_riders_millions,
    a.no2_ppb,
    a.pm25_mcg_per_m3,
    a.o3_ppb,
    CASE
        WHEN r.year = 2020 THEN 'pandemic_trough'
        WHEN r.year BETWEEN 2021 AND 2023 THEN 'recovery'
        WHEN r.year >= 2024 THEN 'post_recovery'
        ELSE 'pre_pandemic'
    END                                                    AS era,
    CASE
        WHEN a.pm25_mcg_per_m3 IS NULL THEN NULL
        ELSE round(a.pm25_mcg_per_m3 / nullif(a.no2_ppb, 0), 3)
    END                                                    AS pm25_no2_ratio
FROM ridership r
LEFT JOIN air a USING (year)
ORDER BY r.year
