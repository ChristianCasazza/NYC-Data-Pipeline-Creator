# DSNY Monthly Tonnage Data: Investigation Report

**Source:** [DSNY Monthly Tonnage Data](https://data.cityofnewyork.us/City-Government/DSNY-Monthly-Tonnage-Data/ebb7-mvp5)
**Socrata ID:** `ebb7-mvp5`
**Asset:** `nyc_dsny_monthly_tonnage`
**Date:** 2026-03-26

---

## 1. Dataset Overview

Monthly collection tonnages reported by the NYC Department of Sanitation across all 59 community districts and 5 boroughs.

| Metric | Value |
|---|---|
| Total rows | 24,883 |
| Time span | January 1990 -- February 2026 (430 distinct months) |
| Boroughs | 5 (Manhattan, Bronx, Brooklyn, Queens, Staten Island) |
| Community districts | 59 |
| Update frequency | Monthly (1st of each month) |

### Schema

| Column | Type | Description |
|---|---|---|
| `month` | String | Year and month in `YYYY / MM` format |
| `borough` | String | NYC borough name |
| `community_district` | String | Sanitation district code (01-18) |
| `refuse_tons_collected` | Float64 | Curbside refuse/trash |
| `paper_tons_collected` | Float64 | Recyclable paper, newspapers, corrugated |
| `mgp_tons_collected` | Float64 | Metal, glass, plastic, beverage cartons |
| `res_organics_tons` | Float64 | Residential curbside organics (food waste) |
| `school_organic_tons` | Float64 | School food waste |
| `leaves_organic_tons` | Float64 | Seasonal leaf collection |
| `xmas_tree_tons` | Float64 | Christmas tree collection (January only) |
| `other_organics_tons` | Float64 | Lot cleaning, green market, yard waste, etc. |
| `borough_id` | String | 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens, 5=Staten Island |

### Data Quality Notes

- **No nulls** in `refuse_tons_collected`, `month`, `borough`, `community_district`, `borough_id`
- **High null rates** in organics columns reflect the phased rollout of collection programs:
  - `res_organics_tons`: 86.6% null (program started ~2013, citywide Oct 2024)
  - `school_organic_tons`: 90.1% null
  - `leaves_organic_tons`: 96.5% null (discontinued Oct 2024, folded into curbside organics)
  - `xmas_tree_tons`: 93.2% null (January-only collection)
  - `other_organics_tons`: 95.0% null
- **Paper/MGP recycling** nulls (9.8% / 8.4%) correspond to pre-1993 records before NYC's recycling program launched
- **No duplicate rows** detected
- **Month format** is consistently `YYYY / MM` (9 chars, space-padded)

---

## 2. Key Findings

### 2.1 Citywide Refuse Trends

NYC's total annual refuse tonnage has been remarkably stable over three decades:

| Period | Avg Annual Refuse (tons) | Trend |
|---|---|---|
| 1991-1999 | 2,813,000 | Baseline |
| 2000-2009 | 3,118,000 | +10.8% (post-9/11 surge, construction boom) |
| 2010-2019 | 3,023,000 | Gradual decline from peak |
| 2020-2025 | 3,026,000 | Stable with COVID spike |

**COVID-19 impact (2020 vs 2019):** Citywide refuse rose +4.6% as residential waste surged with work-from-home. The effect was uneven:
- **Staten Island** +9.7% (highest -- suburban, fully residential)
- **Queens** +8.2%
- **Brooklyn** +5.3%
- **Manhattan** -5.0% (only borough with a decline -- lost office/commercial waste)

Recycling also rose in outer boroughs (+12-17%) but fell in Manhattan (-3.7%).

### 2.2 Recycling Diversion Rates

The recycling diversion rate (paper + MGP as % of refuse + recycling) peaked at ~18% in 2020 and has since declined:

| Year | Recycling % | Organics % |
|---|---|---|
| 2015 | 16.3% | 0.4% |
| 2018 | 17.2% | 1.3% |
| 2020 | 18.0% | 0.4% |
| 2022 | 16.8% | 0.7% |
| 2024 | 16.2% | 1.8% |
| 2025 | 16.9% | 3.7% |

The post-2020 recycling decline is partially offset by the rapid growth of organics collection (see Section 2.4).

### 2.3 Seasonality

Monthly patterns (2015-2025 averages) show clear seasonal signals:

| Month | Avg Refuse/District | Notes |
|---|---|---|
| February | 3,583 tons | Annual low (short month, cold weather) |
| June | 4,533 tons | Annual peak (move-outs, spring cleaning) |
| May | 4,473 tons | Second highest |
| December | 4,279 tons | Holiday spike; paper recycling peaks at 527 tons (highest month) |

Summer months (May-September) consistently produce 15-25% more refuse than the February trough.

### 2.4 Organics Collection Rollout

NYC's curbside organics program is the dataset's biggest story. After a pilot phase (2013-2019), COVID-era suspension, and gradual restart, **citywide mandatory curbside organics launched in October 2024**.

**2025 snapshot** (first full year of citywide program):

| Borough | Res. Organics (tons) | Districts Participating | Share of Total Waste |
|---|---|---|---|
| Queens | 37,162 | 14/14 | 6.2% |
| Brooklyn | 26,781 | 18/18 | 3.7% |
| Staten Island | 13,788 | 3/3 | 7.5% |
| Bronx | 10,275 | 12/12 | 3.0% |
| Manhattan | 9,634 | 12/12 | 2.5% |

**All 59 community districts** are now reporting residential organics in 2025. Staten Island leads in diversion share (7.5%), while Manhattan trails (2.5%) -- likely reflecting higher apartment density and lower single-family compliance.

Total organics tonnage growth: 42K tons (2023) -> 63K tons (2024) -> 128K tons (2025), a **3x increase in two years**.

### 2.5 Community District Rankings

**Highest refuse districts** (2021-2025 avg monthly tons):

| Rank | Borough | District | Avg Monthly Refuse | Recycling Rate |
|---|---|---|---|---|
| 1 | Queens | 07 (Flushing) | 8,908 | 16.2% |
| 2 | Brooklyn | 11 (Bensonhurst) | 8,112 | 11.7% |
| 3 | Bronx | 02 (Hunts Point) | 8,019 | 5.2% |
| 4 | Queens | 12 (Jamaica) | 6,982 | 16.6% |
| 5 | Brooklyn | 12 (Borough Park) | 6,815 | 13.3% |

**Best recycling districts** (highest recycling rate):

| Rank | Borough | District | Recycling Rate | Avg Monthly Refuse |
|---|---|---|---|---|
| 1 | Brooklyn | 06 (Park Slope) | 28.9% | 2,238 |
| 2 | Manhattan | 01 (Financial District) | 28.4% | 1,495 |
| 3 | Manhattan | 07 (Upper West Side) | 26.8% | 4,536 |
| 4 | Manhattan | 06 (Chelsea/Greenwich) | 26.0% | 3,182 |
| 5 | Brooklyn | 02 (Brooklyn Heights) | 25.5% | 2,728 |

**Notable gap:** Bronx 02 (Hunts Point) has the 3rd highest refuse volume but the lowest recycling rate citywide at 5.2% -- a 6x disparity vs. the top recyclers. This points to significant equity gaps in recycling infrastructure and outreach.

---

## 3. Downstream SQL Assets

Four analytics assets were created as downstream SQL transformations of `nyc_dsny_monthly_tonnage`, all materialized to `data/clean/`:

| Asset | Rows | Description |
|---|---|---|
| `dsny_tonnage_annual_summary` | 37 | Citywide annual totals, recycling/organics diversion rates |
| `dsny_tonnage_borough_monthly` | 2,128 | Borough-level monthly aggregates with derived year/month |
| `dsny_tonnage_district_rankings` | 59 | Community district rankings by refuse volume and recycling rate (last 5 years) |
| `dsny_tonnage_organics_rollout` | 70 | Borough-by-year organics adoption tracking (2013+) |

### Asset Lineage

```
nyc_dsny_monthly_tonnage (24,883 rows)
  |
  +-- dsny_tonnage_annual_summary (37 rows)
  +-- dsny_tonnage_borough_monthly (2,128 rows)
  +-- dsny_tonnage_district_rankings (59 rows)
  +-- dsny_tonnage_organics_rollout (70 rows)
```

SQL files: `src/opendata_eda/sql_assets/dsny_tonnage_*.sql`

---

## 4. Potential Next Steps

1. **Organics impact analysis:** Cross-reference organics rollout with refuse decline -- is curbside organics actually reducing landfill tonnage, or is it additive?
2. **Per-capita normalization:** Join with census population data by community district to compute tons-per-capita metrics
3. **Recycling equity mapping:** Geographic analysis of recycling rate disparities across income/demographic lines
4. **Seasonal decomposition:** Time-series decomposition to separate trend, seasonal, and residual components
5. **Forecasting:** Project 2026 full-year tonnage based on seasonal patterns and organics growth trajectory
