/*---
name: nyc_air_quality_annual
source: querystation
description: >
  Annual NYC air quality indicators (NO2, PM 2.5, Ozone) pulled from
  QueryStation's lake.nyc_environment.air_quality table.
  Averages across all tracked neighborhoods; one row per indicator-year.
group: querystation__environment
tags:
  domain: environment
  geographic_scope: nyc
  stage: analytics
---*/

SELECT
    year,
    name                             AS indicator,
    measure_info                     AS units,
    count(*)                         AS observations,
    count(DISTINCT geo_place_name)   AS neighborhoods,
    round(avg(data_value), 3)        AS avg_value,
    round(min(data_value), 3)        AS min_value,
    round(max(data_value), 3)        AS max_value,
    round(stddev(data_value), 3)     AS stddev_value
FROM lake.nyc_environment.air_quality
WHERE name IN ('Nitrogen dioxide (NO2)', 'Fine particles (PM 2.5)', 'Ozone (O3)')
  AND measure = 'Mean'
  AND year >= 2018
GROUP BY 1, 2, 3
ORDER BY indicator, year
