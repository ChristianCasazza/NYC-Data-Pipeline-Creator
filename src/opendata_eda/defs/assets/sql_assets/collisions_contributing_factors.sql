/*---
name: collisions_contributing_factors
description: >
  Contributing factor analysis: each factor ranked by total crash involvement,
  with injury count, fatality count, injury rate, and fatality rate per 10k.
  Unpivots vehicle 1-5 contributing factors into a single column.
deps:
  - nyc_motor_vehicle_collisions
group: nyc__public_safety
tags:
  domain: public_safety
  geographic_scope: nyc
  stage: analytics
---*/

WITH _unpivoted AS (
    SELECT collision_id, contributing_factor_vehicle_1 AS factor,
           number_of_persons_injured, number_of_persons_killed
    FROM nyc_motor_vehicle_collisions WHERE contributing_factor_vehicle_1 IS NOT NULL
    UNION ALL
    SELECT collision_id, contributing_factor_vehicle_2,
           number_of_persons_injured, number_of_persons_killed
    FROM nyc_motor_vehicle_collisions WHERE contributing_factor_vehicle_2 IS NOT NULL
    UNION ALL
    SELECT collision_id, contributing_factor_vehicle_3,
           number_of_persons_injured, number_of_persons_killed
    FROM nyc_motor_vehicle_collisions WHERE contributing_factor_vehicle_3 IS NOT NULL
    UNION ALL
    SELECT collision_id, contributing_factor_vehicle_4,
           number_of_persons_injured, number_of_persons_killed
    FROM nyc_motor_vehicle_collisions WHERE contributing_factor_vehicle_4 IS NOT NULL
    UNION ALL
    SELECT collision_id, contributing_factor_vehicle_5,
           number_of_persons_injured, number_of_persons_killed
    FROM nyc_motor_vehicle_collisions WHERE contributing_factor_vehicle_5 IS NOT NULL
)

SELECT
    factor                                                       AS contributing_factor,
    count(*)                                                     AS crash_involvements,
    count(DISTINCT collision_id)                                 AS distinct_crashes,
    round(sum(coalesce(number_of_persons_injured, 0)), 0)        AS total_injured,
    round(sum(coalesce(number_of_persons_killed, 0)), 0)         AS total_killed,
    round(100.0 * sum(coalesce(number_of_persons_injured, 0))
        / nullif(count(*), 0), 2)                                AS injury_rate_pct,
    round(10000.0 * sum(coalesce(number_of_persons_killed, 0))
        / nullif(count(*), 0), 2)                                AS fatality_rate_per_10k,
    round(100.0 * count(*)
        / nullif(sum(count(*)) OVER (), 0), 2)                   AS pct_of_all_involvements
FROM _unpivoted
WHERE factor != 'Unspecified'
GROUP BY 1
ORDER BY crash_involvements DESC
