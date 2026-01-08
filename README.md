# Netflix-Streaming-Trends-Analysis-with-PySpark

#**Overview**

This repository contains the code and analysis for an Applied Data Science assignment on processing and querying Netflix's Top 10 TV shows and films dataset using PySpark. The project leverages Spark DataFrames and SQL to explore global streaming habits, separating films and TV data, handling nulls, aggregating rankings, and identifying top performers by weeks in Top 10 or #1 spots. Covering data from 2021–2022 across 50+ countries, it uncovers trends like regional preferences (e.g., K-dramas in Asia) and longevity of hits (e.g., "Stranger Things" in the UK).

**Key goals:**

Clean and partition data for efficient querying.
Compute metrics like weeks in Top 10 per title/season/country.
Generate insights on popularity (e.g., top films at #1 per country).
Demonstrate scalable big data processing for ~7M rows.

Findings: US/UK favor Western originals; Asia boosts K-content; films like "The Gray Man" dominate short bursts, while TV seasons (e.g., "Pasión de Gavilanes") sustain longer.

#**Dataset Descriptions**
Netflix Top 10 Data

Scope: Weekly rankings of Top 10 films and TV shows per country from mid-2021 to late 2022, aggregated from Nielsen data.
Structure: Columns – country_name (e.g., "Argentina"), country_iso2 (e.g., "AR"), week (YYYY-MM-DD), category (Films/TV), weekly_rank (1-10), show_title (e.g., "Look Both Ways"), season_title (e.g., "Season 1" or null for films), cumulative_weeks_in_top_10 (running total).
Characteristics: Multi-country (global coverage), temporal (weekly granularity), categorical (films vs. TV), with nulls in season_title for non-seasonal content. Reveals spikes (e.g., new releases) and plateaus (long-runners).
Size: ~7M rows; high cardinality in titles (~5k unique) and weeks (~100).
Use: Enables cross-country comparisons; e.g., "Purple Hearts" topped in 20+ countries but faded quickly.

#**Methodology**

**Data Preparation:**
Load CSV with header/inferred schema via spark.read.csv().
Partition: Filter category == "Films" (filmsDF) vs. "TV" (tvDF).
Clean Films: Drop season_title (irrelevant).
Clean TV: Fill null season_title with show_title using F.when(F.isnull(...), ...).otherwise(...).
Verification: show() and printSchema() for previews.

**Querying & Aggregation:**
Filters: where() for specifics (e.g., "Stranger Things" in UK).
Groups: groupBy() on title/season/country; agg(F.countDistinct("week")) for unique weeks.
Rankings: F.when(weekly_rank == 1, 1).otherwise(0) for #1 indicators; sum() for totals.
Top-N: orderBy(desc("metric")).limit(N) or window functions (max_by() via agg).
SQL Equiv: All via DataFrame API; extensible to spark.sql().

#**Evaluation:**
Outputs: DataFrames with show() for inspection.
Scalability: Spark handles distributed joins/aggs; no explicit metrics but implicit via query efficiency.


#**Key Results**

Ex2: Stranger Things in UK: 52 weeks total across seasons (e.g., Season 4: 10 weeks).
Ex3: Top 25 TV Seasons in UK: Led by "The Crown: Season 1" (18 weeks); "Stranger Things" variants in top 5.
Ex4: Young Sheldon Seasons: Season 1 topped in US (12 weeks); Season 5 in Canada/Australia.
Ex5: Top Film per Country: "The Gray Man" in 15 countries (e.g., US: 8 weeks); "Purple Hearts" in LATAM.
Ex6: Top #1 Films per Country: "Don't Look Up" dominated US/UK (5 weeks); "Squid Game" (TV proxy) in global south.

#**Insights & Applications**

Trends: Films peak short-term (1-4 weeks at #1); TV seasons endure (e.g., K-dramas in Vietnam: 6+ weeks). Regional biases: Western blockbusters in EU/NA; local/Asian content elsewhere.
Business Value: Informs Netflix content strategy (e.g., prioritize sequels for retention); A/B test regional rollouts.
Limitations: Weekly aggregates miss intra-week dynamics; no viewer demographics—future: Join with user data.

#**Tech Stack**

Big Data: PySpark (DataFrames, SQL functions).
Environment: Colab/Spark 3.3 (local mode for dev).
Utils: F for transformations; no viz (focus on queries).
