/*---
name: dsny_tonnage_borough_monthly
description: >
  Monthly tonnage aggregated by borough with derived year/month columns and
  total waste stream calculations. Base table for borough-level trend analysis.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
  geographic_scope: nyc
  stage: analytics
---*/

SELECT
    month,
    TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year,
    TRY_CAST(trim(split_part(month, ' / ', 2)) AS INT) AS month_num,
    borough,
    borough_id,

    round(sum(refuse_tons_collected), 1)                AS refuse_tons,
    round(sum(coalesce(paper_tons_collected, 0)), 1)    AS paper_tons,
    round(sum(coalesce(mgp_tons_collected, 0)), 1)      AS mgp_tons,
    round(sum(coalesce(res_organics_tons, 0)
        + coalesce(school_organic_tons, 0)
        + coalesce(leaves_organic_tons, 0)
        + coalesce(xmas_tree_tons, 0)
        + coalesce(other_organics_tons, 0)), 1)         AS organics_tons,

    -- Total collected
    round(sum(refuse_tons_collected
        + coalesce(paper_tons_collected, 0)
        + coalesce(mgp_tons_collected, 0)
        + coalesce(res_organics_tons, 0)
        + coalesce(school_organic_tons, 0)
        + coalesce(leaves_organic_tons, 0)
        + coalesce(xmas_tree_tons, 0)
        + coalesce(other_organics_tons, 0)), 1)         AS total_tons,

    -- Diversion rate
    round(100.0 * sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0), 1)  AS recycling_pct,

    count(DISTINCT community_district)                  AS districts

FROM nyc_dsny_monthly_tonnage
GROUP BY month, borough, borough_id
ORDER BY month DESC, borough
