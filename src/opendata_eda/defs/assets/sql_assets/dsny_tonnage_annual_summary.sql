/*---
name: dsny_tonnage_annual_summary
description: >
  Citywide annual summary of DSNY collection tonnage with diversion rates.
  Aggregates refuse, paper, MGP, and all organics streams by year.
deps:
  - nyc_dsny_monthly_tonnage
group: nyc__sanitation
tags:
  domain: sanitation
  geographic_scope: nyc
  stage: analytics
---*/

SELECT
    TRY_CAST(trim(split_part(month, ' / ', 1)) AS INT) AS year,
    count(DISTINCT community_district)                  AS districts_reporting,
    count(*)                                            AS records,

    -- Core streams
    round(sum(refuse_tons_collected), 0)                AS total_refuse_tons,
    round(sum(coalesce(paper_tons_collected, 0)), 0)    AS total_paper_tons,
    round(sum(coalesce(mgp_tons_collected, 0)), 0)      AS total_mgp_tons,

    -- Organics breakdown
    round(sum(coalesce(res_organics_tons, 0)), 0)       AS total_res_organics_tons,
    round(sum(coalesce(school_organic_tons, 0)), 0)     AS total_school_organics_tons,
    round(sum(coalesce(leaves_organic_tons, 0)), 0)     AS total_leaves_tons,
    round(sum(coalesce(xmas_tree_tons, 0)), 0)          AS total_xmas_tree_tons,
    round(sum(coalesce(other_organics_tons, 0)), 0)     AS total_other_organics_tons,

    -- Aggregates
    round(sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0)), 0) AS total_recycling_tons,
    round(sum(coalesce(res_organics_tons, 0) + coalesce(school_organic_tons, 0)
        + coalesce(leaves_organic_tons, 0) + coalesce(xmas_tree_tons, 0)
        + coalesce(other_organics_tons, 0)), 0)         AS total_organics_tons,

    -- Grand total
    round(sum(refuse_tons_collected
        + coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0)
        + coalesce(res_organics_tons, 0) + coalesce(school_organic_tons, 0)
        + coalesce(leaves_organic_tons, 0) + coalesce(xmas_tree_tons, 0)
        + coalesce(other_organics_tons, 0)), 0)         AS grand_total_tons,

    -- Diversion rates
    round(100.0 * sum(coalesce(paper_tons_collected, 0) + coalesce(mgp_tons_collected, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0), 1)  AS recycling_diversion_pct,

    round(100.0 * sum(coalesce(res_organics_tons, 0) + coalesce(school_organic_tons, 0)
        + coalesce(leaves_organic_tons, 0) + coalesce(xmas_tree_tons, 0)
        + coalesce(other_organics_tons, 0))
        / nullif(sum(refuse_tons_collected + coalesce(paper_tons_collected, 0)
            + coalesce(mgp_tons_collected, 0)), 0), 1)  AS organics_diversion_pct

FROM nyc_dsny_monthly_tonnage
GROUP BY 1
ORDER BY 1
