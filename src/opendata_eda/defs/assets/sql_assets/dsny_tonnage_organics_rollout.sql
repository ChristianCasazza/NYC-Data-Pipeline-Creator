/*---
name: dsny_tonnage_organics_rollout
description: >
  Tracks the rollout of NYC's curbside organics collection program by borough
  and year. Shows adoption rates, tonnage growth, and district participation.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
  geographic_scope: nyc
  stage: analytics
---*/

WITH _parsed AS (
    SELECT
        *,
        TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year,
        coalesce(res_organics_tons, 0)
            + coalesce(school_organic_tons, 0)
            + coalesce(leaves_organic_tons, 0)
            + coalesce(xmas_tree_tons, 0)
            + coalesce(other_organics_tons, 0)            AS all_organics
    FROM nyc_dsny_monthly_tonnage
    WHERE TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) >= 2013
)

SELECT
    borough,
    year,
    count(DISTINCT community_district)                    AS total_districts,
    count(DISTINCT CASE WHEN res_organics_tons > 0
          THEN community_district END)                    AS districts_with_res_organics,
    count(CASE WHEN res_organics_tons > 0 THEN 1 END)    AS district_months_with_organics,

    round(sum(coalesce(res_organics_tons, 0)), 0)         AS residential_organics_tons,
    round(sum(coalesce(school_organic_tons, 0)), 0)       AS school_organics_tons,
    round(sum(coalesce(leaves_organic_tons, 0)), 0)       AS leaves_tons,
    round(sum(all_organics), 0)                           AS total_organics_tons,

    round(sum(refuse_tons_collected), 0)                  AS total_refuse_tons,

    round(100.0 * sum(all_organics)
        / nullif(sum(refuse_tons_collected + all_organics), 0), 2) AS organics_share_pct

FROM _parsed
GROUP BY borough, year
ORDER BY borough, year
