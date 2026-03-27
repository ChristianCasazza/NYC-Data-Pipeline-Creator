/*---
name: dsny_tonnage_district_rankings
description: >
  Community district rankings by average monthly refuse tonnage (last 5 full years).
  Includes per-capita proxy metrics and recycling rates per district.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
  geographic_scope: nyc
  stage: analytics
---*/

WITH _recent AS (
    SELECT
        *,
        TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year
    FROM nyc_dsny_monthly_tonnage
    WHERE TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT)
          BETWEEN (SELECT max(TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT)) - 5
                   FROM nyc_dsny_monthly_tonnage)
          AND     (SELECT max(TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT)) - 1
                   FROM nyc_dsny_monthly_tonnage)
)

SELECT
    borough,
    community_district,
    count(*)                                             AS months_observed,

    round(avg(refuse_tons_collected), 1)                 AS avg_monthly_refuse,
    round(avg(coalesce(paper_tons_collected, 0)), 1)     AS avg_monthly_paper,
    round(avg(coalesce(mgp_tons_collected, 0)), 1)       AS avg_monthly_mgp,
    round(avg(coalesce(res_organics_tons, 0)), 1)        AS avg_monthly_organics,

    round(sum(refuse_tons_collected), 0)                 AS total_refuse,
    round(sum(coalesce(paper_tons_collected, 0)
        + coalesce(mgp_tons_collected, 0)), 0)           AS total_recycling,

    round(100.0 * sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0), 1)   AS recycling_rate_pct,

    rank() OVER (ORDER BY avg(refuse_tons_collected) DESC) AS refuse_rank_desc,
    rank() OVER (ORDER BY
        100.0 * sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0) DESC)  AS recycling_rank_desc

FROM _recent
GROUP BY borough, community_district
ORDER BY avg_monthly_refuse DESC
