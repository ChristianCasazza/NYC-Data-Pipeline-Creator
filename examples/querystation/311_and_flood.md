# Cross-Analysis: NYC 311 Service Requests x FloodNet Flooding Events

**Sources:**
- [NYC 311 Service Requests](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) — `lake.nyc_operations.service_requests_311`
- [FloodNet Flooding Events](https://data.cityofnewyork.us/Environment/FloodNet-Street-Flooding-Events-Measured-by-FloodN/aq7i-eu5q) — `lake.nyc_environment.floodnet_events_joined`
- [FloodNet Sensor Metadata](https://data.cityofnewyork.us/Environment/FloodNet-Sensor-Deployment-Metadata/kb2e-tjy3) — `lake.nyc_environment.floodnet_sensor_metadata`

**Date:** 2026-03-26

---

## 1. Methodology

### Join Strategy

There is no direct foreign key between 311 complaints and FloodNet events. The two datasets were joined using **shared geographic and temporal dimensions**:

| Join Key | 311 Column | FloodNet Column | Notes |
|---|---|---|---|
| Borough | `borough` | `borough` | Matched via `UPPER()` (casing differs) |
| Year + Month | `year`, `month` | `flood_year`, `flood_month` | Pre-computed temporal columns on both tables |
| Zip code | `incident_zip` | `zipcode` | Most granular geographic join available |

Both datasets include **standardized enrichment columns** added by the pipeline:

| Column | 311 | FloodNet | Description |
|---|---|---|---|
| Year | `year` | `flood_year` | Extracted year (INT) |
| Month | `month` | `flood_month` | Extracted month 1–12 (INT) |
| Hour | `hour` | `flood_hour` | Hour of day 0–23 (INT) |
| Season | — | `flood_season` | winter, spring, summer, fall |
| Overnight | `is_overnight` | `flood_is_overnight` | True if between 10 PM – 5 AM |
| Severity | — | `flood_severity` | minor, moderate, major, severe |

Three temporal granularities were used:
- **Monthly** — joined on `year`/`month` and `flood_year`/`flood_month`
- **Daily** — `DATE_TRUNC('day', ...)` for same-day and next-day lag analysis (no pre-computed day column exists)
- **Hourly** — `hour` and `flood_hour` for time-of-day profiles

### 311 Complaint Filtering

Water/flood-related complaints were identified by filtering `complaint_type` to: `Sewer`, `Standing Water`, `Water System`, and `Water Quality`. These were selected as the most directly flood-relevant types from 203 total complaint categories.

### Limitations

- **Correlation, not causation** — co-occurrence in the same borough on the same day does not prove floods caused the complaints
- **Borough is coarse** — a flood in southeast Queens and a sewer complaint in northwest Queens get matched. Zip-level joins are more precise but FloodNet only covers ~20 zip codes
- **Sensor coverage bias** — FloodNet has sensors in limited neighborhoods, so absence of flood events does not mean absence of flooding
- **311 type filtering is manual** — other complaint types like `Street Condition` (from washouts) may also be flood-related but were excluded

---

## 2. Dataset Profiles

### 311 Service Requests

| Metric | Value |
|---|---|
| Total rows | 7,996,750 |
| Time span | 2024–2026 |
| Boroughs | 6 (5 + Unspecified) |
| Agencies | 16 |
| Complaint types | 203 |
| Temporal columns | `year`, `month`, `quarter`, `day_of_week`, `hour`, `is_overnight` |

**Top complaint types:**

| Complaint Type | Count |
|---|---|
| Illegal Parking | 1,209,999 |
| Noise - Residential | 917,197 |
| HEAT/HOT WATER | 738,787 |
| Blocked Driveway | 389,909 |
| Noise - Street/Sidewalk | 350,581 |

**Water-related complaint types (used in this analysis):**

| Complaint Type | Count | Notes |
|---|---|---|
| Water System | 160,694 | DEP water main/hydrant issues |
| Sewer | 57,778 | Sewer backups, overflows |
| Water Quality | 2,866 | Discolored/odorous water |
| Standing Water | 2,714 | Pooled water on streets |

**Borough breakdown:**

| Borough | Requests | Avg Resolution (hrs) |
|---|---|---|
| Brooklyn | 2,393,875 | 221.9 |
| Queens | 1,917,757 | 225.4 |
| Bronx | 1,763,628 | 163.1 |
| Manhattan | 1,615,464 | 280.9 |
| Staten Island | 300,058 | 251.4 |

### FloodNet Events (Joined with Sensor Metadata)

| Metric | Value |
|---|---|
| Total events | 1,887 |
| Time span | Nov 2020 – Jan 2026 |
| Overlap with 311 | Jan 2024 – Jan 2026 |
| Boroughs with sensors | 5 |
| Temporal columns | `flood_year`, `flood_month`, `flood_hour`, `flood_season`, `flood_is_overnight` |
| Enrichment columns | `flood_severity`, `is_tidally_influenced`, `rise_rate_in_per_min`, `drain_rate_in_per_min`, `flood_volume_index` |

**Borough breakdown (`flood_year >= 2024`):**

| Borough | Flood Events | Avg Depth (in) | Avg Duration (min) |
|---|---|---|---|
| Queens | 750 | 5.5 | 149 |
| Bronx | 336 | 4.8 | 196 |
| Brooklyn | 124 | 4.2 | 77 |
| Staten Island | 102 | 3.7 | 298 |
| Manhattan | 13 | 3.7 | 61 |

**Severity distribution (`flood_year >= 2024`, using `flood_severity`):**

| Borough | Minor (<4in) | Moderate (4–12in) | Major (12–24in) | Severe (24in+) | Avg Duration (major+) |
|---|---|---|---|---|---|
| Queens | 347 | 319 | 80 | 4 | 282–413 min |
| Bronx | 163 | 164 | 7 | 2 | 301–618 min |
| Brooklyn | 78 | 34 | 12 | 0 | 113 min |
| Staten Island | 69 | 32 | 1 | 0 | 48 min |
| Manhattan | 7 | 6 | 0 | 0 | — |

---

## 3. Key Findings

### 3.1 Flood Days Drive More Water-Related 311 Complaints

Flood days were bucketed by the number of FloodNet events recorded in a borough on that day. On major flood days (10+ events), water-related 311 complaints are **2.1x higher** than low/no-flood days, and their share of total complaints nearly doubles.

| Flood Intensity | Days | Avg Water 311s | Avg Total 311s | Water % of Total |
|---|---|---|---|---|
| Major (10+ events) | 19 | **167** | 2,562 | **6.51%** |
| Moderate (5–9) | 42 | 88 | 1,959 | 4.51% |
| Minor (3–4) | 62 | 82 | 2,051 | 3.98% |
| Low/None (0–2) | 364 | 78 | 2,105 | 3.72% |

### 3.2 Queens is the Epicenter

Queens accounts for **~56% of all flood events** (2024+) and dominates every major flood day. The single worst day was **October 30, 2025**: 56 flood events in Queens and 46 in Brooklyn, with 623 and 795 water complaints filed respectively.

**Top flood days (by event count, with `flood_severity` included):**

| Date | Borough | Flood Events | Max Depth (in) | Worst Severity | Water 311s (same day) | Water 311s (next day) |
|---|---|---|---|---|---|---|
| 2025-10-30 | Queens | 56 | 22+ | moderate | 623 | 277 |
| 2025-10-30 | Brooklyn | 46 | 23+ | moderate | 795 | 194 |
| 2025-10-13 | Queens | 21 | 15+ | moderate | 57 | 58 |
| 2024-09-21 | Queens | 16 | 21+ | moderate | 72 | 49 |
| 2024-03-10 | Queens | 16 | 26+ | severe | 120 | 90 |
| 2024-11-15 | Queens | 16 | 21+ | moderate | 120 | 43 |
| 2025-08-01 | Brooklyn | 16 | 13.5 | moderate | 186 | 56 |

### 3.3 Complaint Spikes Persist the Next Day

After major flood events, next-day water complaints remain elevated. On Oct 30, 2025 in Queens, complaints went from 623 same-day to 277 next-day — still 3.5x the typical daily average. This suggests infrastructure strain (sewer backups, standing water) extends beyond the immediate event.

### 3.4 Seasonal and Overnight Patterns

Using `flood_season` and `flood_is_overnight`:

| Season | Flood Events | Avg Depth (in) | Overnight % | Top Borough |
|---|---|---|---|---|
| Fall | 441 | 6.2 | 16.5% | Queens (284) |
| Spring | 309 | 5.8 | 57.0% | Queens (190) |
| Summer | 397 | 3.9 | 61.3% | Queens (160) |
| Winter | 178 | 5.6 | 17.5% | Queens (116) |

Key patterns:
- **Fall has the most events and deepest floods** — October is the peak month
- **Summer and spring floods are overwhelmingly overnight** (61% and 57%) — afternoon/evening storms produce flooding that peaks in late-night hours
- **Winter floods are rarely overnight** (17.5%) — more likely daytime rain/snowmelt events

### 3.5 Hour-of-Day: Floods vs. 311 Complaints

**Flood events by `flood_hour`:**

| Time Block | Flood Events | Avg Depth | Notes |
|---|---|---|---|
| 11 PM – 1 AM | 332 (peak) | 5.0 in | Storm-driven nighttime flooding |
| 7 PM | 108 | 6.8 in | Deepest average — evening storm onset |
| 10 AM – 11 AM | 103 | 7.6 in | Deepest morning floods (tidal + rain) |
| 7 AM – 8 AM | 24 (trough) | 3.4 in | Fewest events |

**311 water complaints by `hour`:**

| Time Block | Water Complaints | Notes |
|---|---|---|
| 3 PM (peak) | 16,089 | Business hours peak |
| 9 AM – 5 PM | ~15,000/hr | Steady daytime plateau |
| 3 AM (trough) | 734 | Minimal overnight reporting |

The mismatch is significant: **floods peak overnight but complaints peak during business hours**, meaning there is a systematic lag of 6–12 hours between when flooding occurs and when it gets reported to 311.

### 3.6 Zip-Code Hotspots

The most flood-prone zip codes and their corresponding 311 profiles:

| Zip | Flood Events | Avg Depth | Water 311s | Water % of Total 311 |
|---|---|---|---|---|
| 10464 (City Island, Bronx) | 286 | 5.4 in | 217 | 3.52% |
| 11414 (Howard Beach, Queens) | 224 | 6.8 in | 1,280 | 4.12% |
| 11693 (Far Rockaway, Queens) | 191 | 6.7 in | 592 | 6.14% |
| 11422 (Rosedale, Queens) | 175 | 4.6 in | 1,702 | **9.40%** |
| 11691 (Far Rockaway, Queens) | 90 | 4.5 in | 1,743 | 4.28% |
| 10306 (New Dorp, SI) | 59 | 3.7 in | 2,566 | 7.60% |

Zip code **11422 (Rosedale)** stands out: 9.4% of all 311 complaints are water-related — nearly 3x the citywide average. This is a low-lying area near JFK airport with known drainage issues.

### 3.7 Brooklyn's Sensor Blind Spot

Brooklyn generates the 2nd most 311 complaints citywide but has only **124 flood events** (2024+) compared to Queens' 750. This is almost certainly a sensor coverage gap, not an absence of flooding. Evidence:

- Brooklyn's October 2025 spike (48 events) coincided with new sensor deployments
- Brooklyn has consistently high `Sewer` and `Standing Water` complaint volumes relative to its flood event count
- The borough's water complaint rate (~3.8% of total) is comparable to Queens despite having far fewer measured events

---

## 4. SQL Queries

All queries use the standardized temporal columns (`year`, `month`, `hour`, `is_overnight` on 311; `flood_year`, `flood_month`, `flood_hour`, `flood_season`, `flood_is_overnight`, `flood_severity` on FloodNet) instead of raw `DATE_TRUNC` where possible.

### 4.1 Water-Related 311 Complaint Types

Identify which complaint types are flood/water-relevant:

```sql
SELECT complaint_type, COUNT(*) AS cnt
FROM lake.nyc_operations.service_requests_311
WHERE complaint_type ILIKE '%flood%'
   OR complaint_type ILIKE '%sewer%'
   OR complaint_type ILIKE '%water%'
   OR complaint_type ILIKE '%drain%'
   OR complaint_type ILIKE '%storm%'
GROUP BY 1
ORDER BY 2 DESC
```

### 4.2 FloodNet Events by Borough

```sql
SELECT borough,
       COUNT(*) AS flood_events,
       ROUND(AVG(max_depth_inches), 1) AS avg_depth,
       ROUND(AVG(duration_mins), 0) AS avg_duration_mins
FROM lake.nyc_environment.floodnet_events_joined
WHERE borough IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
```

### 4.3 FloodNet Date Range

```sql
SELECT MIN(flood_start_time) AS earliest,
       MAX(flood_start_time) AS latest,
       COUNT(*) AS total_events
FROM lake.nyc_environment.floodnet_events_joined
```

### 4.4 Monthly Borough-Level Join: Floods vs. Water Complaints

Uses `flood_year`/`flood_month` and `year`/`month` instead of `DATE_TRUNC`:

```sql
WITH flood_monthly AS (
    SELECT borough, flood_year, flood_month,
           COUNT(*) AS flood_events,
           ROUND(AVG(max_depth_inches), 1) AS avg_depth
    FROM lake.nyc_environment.floodnet_events_joined
    WHERE borough IS NOT NULL
      AND flood_year >= 2024
    GROUP BY 1, 2, 3
),
complaints_monthly AS (
    SELECT borough, year, month,
           COUNT(*) AS total_311,
           SUM(CASE WHEN complaint_type IN ('Sewer', 'Standing Water', 'Water System')
                    THEN 1 ELSE 0 END) AS water_complaints
    FROM lake.nyc_operations.service_requests_311
    WHERE borough IN ('BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN', 'STATEN ISLAND')
    GROUP BY 1, 2, 3
)
SELECT f.borough, f.flood_year, f.flood_month,
       f.flood_events, f.avg_depth,
       c.water_complaints, c.total_311
FROM flood_monthly f
JOIN complaints_monthly c
  ON UPPER(f.borough) = UPPER(c.borough)
 AND f.flood_year = c.year
 AND f.flood_month = c.month
ORDER BY f.flood_year, f.flood_month, f.borough
```

### 4.5 Zip-Code Join: Flood Events vs. 311 Water Complaints

Uses `flood_year` for time filtering:

```sql
WITH flood_zips AS (
    SELECT zipcode,
           COUNT(*) AS flood_events,
           ROUND(AVG(max_depth_inches), 1) AS avg_depth,
           ROUND(AVG(duration_mins), 0) AS avg_duration
    FROM lake.nyc_environment.floodnet_events_joined
    WHERE zipcode IS NOT NULL
      AND flood_year >= 2024
    GROUP BY 1
),
complaint_zips AS (
    SELECT incident_zip AS zipcode,
           COUNT(*) AS total_311,
           SUM(CASE WHEN complaint_type IN ('Sewer', 'Standing Water',
                         'Water System', 'Water Quality')
                    THEN 1 ELSE 0 END) AS water_complaints
    FROM lake.nyc_operations.service_requests_311
    WHERE incident_zip IS NOT NULL
    GROUP BY 1
)
SELECT f.zipcode, f.flood_events, f.avg_depth, f.avg_duration,
       c.water_complaints, c.total_311,
       ROUND(c.water_complaints * 1.0 / NULLIF(c.total_311, 0) * 100, 2) AS water_pct
FROM flood_zips f
JOIN complaint_zips c ON f.zipcode = c.zipcode
ORDER BY f.flood_events DESC
LIMIT 20
```

### 4.6 Daily Flood Events vs. Same-Day and Next-Day 311 Complaints

Uses `flood_year`, `flood_month`, and `flood_severity` in output (daily grain still requires `DATE_TRUNC` for the join since no pre-computed day column exists):

```sql
WITH flood_days AS (
    SELECT DATE_TRUNC('day', flood_start_time) AS flood_date,
           borough, flood_year, flood_month,
           COUNT(*) AS flood_events,
           MAX(max_depth_inches) AS max_depth,
           MAX(flood_severity) AS worst_severity
    FROM lake.nyc_environment.floodnet_events_joined
    WHERE borough IS NOT NULL
      AND flood_year >= 2024
    GROUP BY 1, 2, 3, 4
    HAVING COUNT(*) >= 3
),
complaints_day AS (
    SELECT DATE_TRUNC('day', created_date) AS day,
           borough,
           SUM(CASE WHEN complaint_type IN ('Sewer', 'Standing Water', 'Water System')
                    THEN 1 ELSE 0 END) AS water_complaints
    FROM lake.nyc_operations.service_requests_311
    WHERE borough IN ('BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN', 'STATEN ISLAND')
    GROUP BY 1, 2
)
SELECT f.flood_date, f.borough, f.flood_year, f.flood_month,
       f.flood_events, f.max_depth, f.worst_severity,
       c.water_complaints,
       c2.water_complaints AS water_complaints_next_day
FROM flood_days f
JOIN complaints_day c
  ON UPPER(f.borough) = UPPER(c.borough)
 AND f.flood_date = c.day
LEFT JOIN complaints_day c2
  ON UPPER(f.borough) = UPPER(c2.borough)
 AND f.flood_date + INTERVAL '1 day' = c2.day
ORDER BY f.flood_events DESC
LIMIT 20
```

### 4.7 Flood Intensity Buckets vs. Average 311 Complaints

Uses `flood_year` for time filtering:

```sql
SELECT
    CASE WHEN f.flood_events >= 10 THEN 'major_flood_day (10+)'
         WHEN f.flood_events >= 5  THEN 'moderate_flood_day (5-9)'
         WHEN f.flood_events >= 3  THEN 'minor_flood_day (3-4)'
         ELSE 'no/low_flood_day (0-2)' END AS flood_intensity,
    COUNT(*) AS days,
    ROUND(AVG(c.water_complaints), 0) AS avg_water_311,
    ROUND(AVG(c.total_311), 0) AS avg_total_311,
    ROUND(AVG(c.water_complaints) * 100.0
          / NULLIF(AVG(c.total_311), 0), 2) AS water_pct_of_total
FROM (
    SELECT DATE_TRUNC('day', flood_start_time) AS day,
           borough, flood_year,
           COUNT(*) AS flood_events
    FROM lake.nyc_environment.floodnet_events_joined
    WHERE borough IS NOT NULL
      AND flood_year >= 2024
    GROUP BY 1, 2, 3
) f
JOIN (
    SELECT DATE_TRUNC('day', created_date) AS day,
           borough,
           SUM(CASE WHEN complaint_type IN ('Sewer', 'Standing Water',
                         'Water System', 'Water Quality')
                    THEN 1 ELSE 0 END) AS water_complaints,
           COUNT(*) AS total_311
    FROM lake.nyc_operations.service_requests_311
    WHERE borough IN ('BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN', 'STATEN ISLAND')
    GROUP BY 1, 2
) c ON UPPER(f.borough) = UPPER(c.borough) AND f.day = c.day
GROUP BY 1
ORDER BY avg_water_311 DESC
```

### 4.8 Flood Severity Distribution by Borough

Uses `flood_severity` and `flood_year`:

```sql
SELECT borough, flood_severity,
       COUNT(*) AS events,
       ROUND(AVG(duration_mins), 0) AS avg_duration,
       ROUND(AVG(max_depth_inches), 1) AS avg_depth
FROM lake.nyc_environment.floodnet_events_joined
WHERE borough IS NOT NULL
  AND flood_year >= 2024
GROUP BY 1, 2
ORDER BY borough,
    CASE flood_severity
        WHEN 'minor' THEN 1
        WHEN 'moderate' THEN 2
        WHEN 'major' THEN 3
        WHEN 'severe' THEN 4
    END
```

### 4.9 Seasonal and Overnight Flood Patterns

Uses `flood_season` and `flood_is_overnight`:

```sql
SELECT flood_season, borough,
       COUNT(*) AS flood_events,
       ROUND(AVG(max_depth_inches), 1) AS avg_depth,
       SUM(CASE WHEN flood_is_overnight THEN 1 ELSE 0 END) AS overnight_events,
       ROUND(SUM(CASE WHEN flood_is_overnight THEN 1 ELSE 0 END) * 100.0
             / COUNT(*), 1) AS overnight_pct
FROM lake.nyc_environment.floodnet_events_joined
WHERE borough IS NOT NULL
  AND flood_year >= 2024
GROUP BY 1, 2
ORDER BY
    CASE flood_season
        WHEN 'winter' THEN 1 WHEN 'spring' THEN 2
        WHEN 'summer' THEN 3 WHEN 'fall' THEN 4 END,
    flood_events DESC
```

### 4.10 Flood Events by Hour of Day

Uses `flood_hour`:

```sql
SELECT flood_hour,
       COUNT(*) AS flood_events,
       ROUND(AVG(max_depth_inches), 1) AS avg_depth
FROM lake.nyc_environment.floodnet_events_joined
WHERE borough IS NOT NULL
  AND flood_year >= 2024
GROUP BY 1
ORDER BY 1
```

### 4.11 311 Water Complaints by Hour of Day

Uses `hour`:

```sql
SELECT hour,
       SUM(CASE WHEN complaint_type IN ('Sewer', 'Standing Water',
                     'Water System', 'Water Quality')
                THEN 1 ELSE 0 END) AS water_complaints,
       COUNT(*) AS total_311
FROM lake.nyc_operations.service_requests_311
WHERE borough IN ('BROOKLYN', 'QUEENS', 'BRONX', 'MANHATTAN', 'STATEN ISLAND')
GROUP BY 1
ORDER BY 1
```

---

## 5. Potential Next Steps

1. **Sensor deployment optimization** — Use 311 sewer/standing water complaint density in zip codes *without* FloodNet sensors to prioritize new installations (especially Brooklyn)
2. **Lag modeling** — Build a 1–3 day lag model between flood events and 311 complaint spikes to forecast surge capacity for DEP and 311 call centers
3. **Overnight flood → morning complaint pipeline** — The 6–12 hour gap between overnight flood peaks (`flood_hour` 23–1) and daytime complaint peaks (`hour` 9–15) could inform proactive DEP dispatch
4. **Depth thresholds** — Determine the flood depth at which 311 complaints meaningfully spike — could inform automated alert triggers
5. **Seasonal decomposition** — Separate weather-driven water complaints (correlated with floods) from infrastructure-driven complaints (year-round baseline) using `flood_season`
6. **Community board resolution equity** — Compare resolution times for water complaints in flood-prone vs. non-flood-prone community boards
7. **Cross-join with MTA ridership** — Do major flood days also suppress subway ridership? Use `lake.nys_transportation.mta_daily_ridership` to measure transit impact
