/*---
name: collisions_borough_monthly
description: >
  Borough-level monthly collision aggregates: crash count, injuries, fatalities,
  and pedestrian/cyclist injury counts. Filters to rows with known borough only.
deps:
  - nyc_motor_vehicle_collisions
group: nyc__public_safety
tags:
  domain: public_safety
  geographic_scope: nyc
  stage: analytics
---*/

SELECT
    borough,
    year(crash_date)                                            AS crash_year,
    month(crash_date)                                           AS crash_month,
    count(*)                                                    AS total_crashes,
    round(sum(coalesce(number_of_persons_injured, 0)), 0)       AS total_injured,
    round(sum(coalesce(number_of_persons_killed, 0)), 0)        AS total_killed,
    round(sum(coalesce(number_of_pedestrians_injured, 0)), 0)   AS pedestrians_injured,
    round(sum(coalesce(number_of_pedestrians_killed, 0)), 0)    AS pedestrians_killed,
    round(sum(coalesce(number_of_cyclist_injured, 0)), 0)       AS cyclists_injured,
    round(sum(coalesce(number_of_cyclist_killed, 0)), 0)        AS cyclists_killed,
    round(sum(coalesce(number_of_motorist_injured, 0)), 0)      AS motorists_injured,
    round(sum(coalesce(number_of_motorist_killed, 0)), 0)       AS motorists_killed,
    round(1.0 * sum(coalesce(number_of_persons_injured, 0))
        / count(*), 3)                                          AS injuries_per_crash,
    round(10000.0 * sum(coalesce(number_of_persons_killed, 0))
        / nullif(count(*), 0), 2)                               AS fatalities_per_10k_crashes
FROM nyc_motor_vehicle_collisions
WHERE borough IS NOT NULL
  AND crash_date IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
