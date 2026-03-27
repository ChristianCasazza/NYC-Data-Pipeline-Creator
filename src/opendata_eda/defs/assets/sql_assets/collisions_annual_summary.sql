/*---
name: collisions_annual_summary
description: >
  Citywide annual collision summary: total crashes, injuries, and fatalities
  broken down by persons, pedestrians, cyclists, and motorists. Includes
  year-over-year change percentages and fatality rate per 10k crashes.
deps:
  - nyc_motor_vehicle_collisions
group: nyc__public_safety
tags:
  domain: public_safety
  geographic_scope: nyc
  stage: analytics
---*/

WITH _annual AS (
    SELECT
        year(crash_date)                                        AS crash_year,
        count(*)                                                AS total_crashes,
        round(sum(coalesce(number_of_persons_injured, 0)), 0)   AS total_injured,
        round(sum(coalesce(number_of_persons_killed, 0)), 0)    AS total_killed,
        round(sum(coalesce(number_of_pedestrians_injured, 0)), 0) AS pedestrians_injured,
        round(sum(coalesce(number_of_pedestrians_killed, 0)), 0)  AS pedestrians_killed,
        round(sum(coalesce(number_of_cyclist_injured, 0)), 0)   AS cyclists_injured,
        round(sum(coalesce(number_of_cyclist_killed, 0)), 0)    AS cyclists_killed,
        round(sum(coalesce(number_of_motorist_injured, 0)), 0)  AS motorists_injured,
        round(sum(coalesce(number_of_motorist_killed, 0)), 0)   AS motorists_killed
    FROM nyc_motor_vehicle_collisions
    WHERE crash_date IS NOT NULL
    GROUP BY 1
)

SELECT
    crash_year,
    total_crashes,
    total_injured,
    total_killed,
    pedestrians_injured,
    pedestrians_killed,
    cyclists_injured,
    cyclists_killed,
    motorists_injured,
    motorists_killed,
    round(10000.0 * total_killed / nullif(total_crashes, 0), 2) AS fatalities_per_10k_crashes,
    round(100.0 * (pedestrians_killed + cyclists_killed)
        / nullif(total_killed, 0), 1)                           AS vulnerable_user_fatality_pct,
    round(100.0 * (total_crashes - lag(total_crashes) OVER (ORDER BY crash_year))
        / nullif(lag(total_crashes) OVER (ORDER BY crash_year), 0), 1) AS crash_yoy_change_pct,
    round(100.0 * (total_killed - lag(total_killed) OVER (ORDER BY crash_year))
        / nullif(lag(total_killed) OVER (ORDER BY crash_year), 0), 1)  AS fatality_yoy_change_pct
FROM _annual
WHERE crash_year >= 2013
ORDER BY crash_year
