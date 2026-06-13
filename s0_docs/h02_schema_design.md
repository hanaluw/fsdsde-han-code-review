# Fintech Intelligent Platform - Gold Zone Schema Design -  Luu Gia Han's coursework proposal in EDAI course

## 1. Goal

Build a business-ready Gold data model for analytics and downstream Natural language processing/Large language model use, backed by implemented Bronze --> Silver --> Gold pipelines with full lineage visibility.


**Approach:** Dimension + Fact + OBT for structured analytics, plus article-level and engagement feature tables for ML/LLM ranking and personalization.

**Coursework requirement:** Design and implement all data pipelines end-to-end. Capture lineage for key datasets using DataHub.

**Storage requirement (cost-focused):** Bronze and Silver layers stored in Delta Lake (local/object storage) for cost efficiency, time-travel, and incremental processing.

**Tech stack:**
- Batch pipelines: PySpark + Delta Lake
- Streaming pipelines: Apache Flink + Delta Lake
- Orchestration: scheduled triggers (cron or Airflow)
- Lineage: DataHub

**Schema naming:**
- Bronze: `raw_` prefix (e.g., `raw_news_articles`)
- Silver: `stg_` prefix (e.g., `stg_news_articles`)
- Gold: `gold_fintech` schema with `dim_`, `fact_`, `obt_`, `feat_` prefixes

**Assumptions**
- **Business objective:** provide reliable, query-efficient Gold datasets for BI dashboards (trending articles, engagement metrics, source analysis) and as a foundation for downstream NLP/LLM retrieval ranking.
- **Decision usage:** Gold tables and features support analytics queries and NLP/LLM re-ranking/scoring; no fully automated hard decisions at this stage.
- **Service level expectation:** Gold and feature data must meet freshness and availability targets defined in Section 1.3.
<!-- - **Explainability:** out of scope for the current phase.
- **Risk and governance:** out of scope for the current phase. -->

**SLA Targets**

| Layer | Target Freshness | Availability |
|-------|-----------------|--------------|
| Bronze (offline) | ≤ 30 min after 10:00 ICT crawl completes | ≥ 99% scheduled-run success/week |
| Bronze (streaming) | ≤ 5 min from Kafka event arrival | ≥ 99% |
| Silver (offline) | ≤ 60 min after Bronze lands | ≥ 99% |
| Silver (streaming) | ≤ 10 min from Bronze streaming ingest | ≥ 99% |
| Gold dimensions | ≤ 2 hours (daily refresh acceptable) | ≥ 99% |
| Gold facts / OBT | ≤ 30 min incremental merge | ≥ 99% |
| `feat_article_offline` | ≤ 60 min (2× daily recompute) | ≥ 99% |
| `feat_article_stream` | ≤ 5 min rolling window | ≥ 99% |

---

## 2. Dimension Tables

| Dimension | Grain | Key Columns | SCD Strategy |
|-----------|-------|-------------|--------------|
| `dim_article` | one per article | `article_key` (SK), `article_id` (BK, sha256), `url`, `title`, `source_key`, `published_date`, `category_primary`, `tag_count`, `has_relevant_article` | SCD1 (update in place; articles rarely change after publish) |
| `dim_source` | one per news source | `source_key` (SK), `source_id` (BK), `source_name`, `domain_name`, `is_active` | SCD1 (near-static) |
| `dim_user` | one per user | `user_key` (SK), `user_id` (BK), `signup_date`, `email_subscription`, `signup_year` | SCD1 (subscription flag may change; track with `updated_ts`) |
| `dim_date` | one per calendar date | `date_key` (yyyymmdd), `calendar_date`, `day_of_week`, `month`, `year`, `is_weekend`, `is_vietnamese_holiday` | Static; pre-populated |
| `dim_event_type` | one per event type | `event_type_key` (SK), `event_type` (BK): `article_view`, `reaction`, `comment_post`, `share` | Static |
| `dim_device_type` | one per device class | `device_type_key` (SK), `device_type`: `mobile`, `desktop`, `tablet` | Static |

**Notes:**
*- SK = surrogate key (warehouse-generated integer), BK = business key (natural identifier from source).*
*- `dim_article.has_relevant_article` is a boolean flag derived from schema evolution handling: `FALSE` for pre-2021 articles, `TRUE` otherwise.*
*- `dim_date` is pre-populated from 2018-01-01 to 2030-12-31.*

---

## 3. Fact Tables

### `fact_article_daily_stats`

**Grain:** one row per article per day.
**Purpose:** pre-aggregated daily metrics for article performance dashboards.

| Column | Type | Notes |
|--------|------|-------|
| `article_key` (FK) | INT | --> `dim_article` |
| `source_key` (FK) | INT | --> `dim_source` |
| `event_date_key` (FK) | INT | --> `dim_date` |
| `total_views` | INT | count of `article_view` events |
| `total_reactions` | INT | count of `reaction` events |
| `total_comments` | INT | count of `comment_post` events |
| `total_shares` | INT | count of `share` events |
| `unique_viewers` | INT | distinct `user_id` values with view events |
| `avg_read_duration_seconds` | FLOAT | average of non-null read durations |
| `facebook_share_count` | INT | shares on Facebook specifically |
| `like_count` | INT | reactions of type `like` |
| `insightful_count` | INT | reactions of type `insightful` |
| `concerned_count` | INT | reactions of type `concerned` |

**Merge key:** (`article_key`, `event_date_key`) - incremental merge daily.

---

## 4. OBT Table

### `obt_article_performance`

**Grain:** one row per article per day.
**Purpose:** fully denormalized table for BI dashboards and ad-hoc queries. Eliminates join complexity for typical patterns (e.g., *"top trending articles this week by source"*, *"engagement breakdown by category"*).
**Merge key:** (`article_id`, `event_date`) - incremental merge every 30 min, driven by streaming engagement updates

**Core columns:**

| Column | Source |
|--------|--------|
| `article_id` | `dim_article` |
| `title` | `dim_article` |
| `published_date` | `dim_article` |
| `source_name` | `dim_source` |
| `domain_name` | `dim_source` |
| `category_primary` | `dim_article` |
| `tag_count` | `dim_article` |
| `has_relevant_article` | `dim_article` |
| `event_date` | `dim_date` |
| `article_age_days` | computed: `event_date - published_date` |
| `is_weekend` | `dim_date` |
| `total_views` | `fact_article_daily_stats` |
| `total_reactions` | `fact_article_daily_stats` |
| `total_comments` | `fact_article_daily_stats` |
| `total_shares` | `fact_article_daily_stats` |
| `unique_viewers` | `fact_article_daily_stats` |
| `avg_read_duration_seconds` | `fact_article_daily_stats` |
| `facebook_share_count` | `fact_article_daily_stats` |
| `engagement_score` | computed: `total_views + 3*total_reactions + 5*total_comments + 2*total_shares` |

---

## 5. Refresh & Data Quality

### 5.1 Refresh SLAs

| Table | Strategy | Frequency |
|-------|----------|-----------|
| Dimension tables | SCD1 upsert | Daily at 11:00 ICT |
| `fact_article_daily_stats` | Incremental merge by (`article_key`, `event_date_key`) | Every 30 min |
| `obt_article_performance` | Incremental merge by (`article_id`, `event_date`) | Every 30 min |

### 5.2 Quality Checks

| Check | Target | Rule |
|-------|--------|------|
| Uniqueness | `dim_article` | `article_id` must be unique |
| Uniqueness | `fact_article_daily_stats` | (`article_key`, `event_date_key`) must be unique |
| Referential integrity | All fact/OBT tables | All FKs must resolve to their dimension |
| Null check | `fact_article_daily_stats` | `article_key`, `event_date_key`, `total_views` must not be null |
| Volume check | `fact_article_daily_stats` | Daily row count within ±50% of 7-day moving average |
| Schema evolution | `stg_news_articles` | `relevant_article` null rate: ~100% for pre-2021 partitions, < 5% for post-2021 |
| Duplicate check | `stg_user_events` | `event_id` dedup rate expected ~2%; alert if > 5% |
| Engagement consistency | `obt_article_performance` | `engagement_score` ≥ `total_views` |

---

## 6. Feature Store

When multiple rows share the same entity key and `event_timestamp`, keep the row with the latest `created_ts`.

### 6.1 `feat_article_offline`

**Grain:** (`article_id`, `event_timestamp`) - recomputed 2× daily after Silver refresh (11:30 + 22:00 ICT).

| Feature | Description |
|---------|-------------|
| `f_article_age_days` | Days since `published_date` |
| `f_article_tag_count` | Number of tags on the article |
| `f_article_category_primary` | Top-level category derived from tags |
| `f_article_has_relevant_link` | Boolean: whether `relevant_article` is populated |

---

### 6.2 `feat_article_stream`

**Grain:** (`article_id`, `event_timestamp`) - materialized every 5 minutes from Flink aggregation.
**Late-data handling:** Flink watermark at max(`event_timestamp`) − 45 minutes.
 
| Feature | Description |
|---------|-------------|
| `f_article_views_1h` | View count in the last 1 hour |
| `f_article_reactions_24h` | Reaction count in the last 24 hours |
| `f_article_comment_count_24h` | Comment count in the last 24 hours |
| `f_article_share_rate_24h` | Shares / views ratio in last 24 hours (0 if no views) |
| `f_burst_activity_flag` | 1 if event falls within a burst window (08:00–08:15 or 12:00–12:15 ICT) |

### 6.3 `NLP Features`

To be computed as a dedicated NLP pipeline once Silver pipelines are stable.
 
| Planned Table | Feature | Method |
|---------------|---------|--------|
| `feat_article_topics` | `f_article_topic_label`, `f_topic_confidence` | BERTopic or LDA |
| `feat_article_entities` | `f_entities_org`, `f_entities_person`, `f_entities_product` | underthesea NER (Vietnamese) |
| `feat_article_sentiment` | `f_sentiment_score`, `f_sentiment_label` | Fine-tuned PhoBERT |

## 7. Data Pipeline Design and Implementation Scope

### 7.0 Pipeline Overview
 
```
[CafeF Crawler]                             [Fintech_user_events]
       |                                              |
Parquet · daily 10:00 ICT                             |
       |                                              |
       ▼                                      Kafka Topic Events
D̶e̶b̶e̶z̶i̶u̶m̶ ̶c̶o̶n̶n̶e̶c̶t̶o̶r                                    |
   P̶o̶s̶t̶g̶r̶e̶S̶Q̶L̶                                         |
  Kafka Topic News                                    |
       |                                              |
       ▼                                              ▼
[Bronze Offline · PySpark]                [Bronze Streaming · Flink]
raw_news_articles                               raw_user_events
raw_users, raw_news_sources               (Delta Lake, append-only)
(Delta Lake, append-only)
       |                                              |
       ▼                                              ▼
[Silver Offline · PySpark]                [Silver Streaming · Flink]
stg_news_articles                               stg_user_events
stg_users, stg_news_sources           (dedup by event_id, watermark, DLQ)
(dedup, schema fix, tag norm)
       |                                              |
       └──────────────────┬───────────────────────────┘
                          ▼
              [Gold Pipelines · PySpark]
              dim_article, dim_source, dim_user
              fact_article_daily_stats
              obt_article_performance
                          |
                          ▼
                 [Feature Pipelines]
         feat_article_offline  (PySpark, 2×/day)
       feat_article_stream   (Flink, continuous)
                          |
                          ▼
                  [Lineage: DataHub]
```

### 7.1 Bronze Ingestion Pipelines

**Offline (PySpark, trigger: 10:30 ICT daily)**
- Validate schema: assert required columns (`article_id`, `url`, `title`, `source_id`, `published_datetime`) are present.
- Add ingest metadata: `ingest_ts`, `batch_id` (date string).
- Write `raw_news_articles`, `raw_users`, `raw_news_sources` to Delta Lake, partitioned by `published_date` / `ingest_date`. Append-only.
**Streaming (Flink, continuous)**
- Deserialize Kafka Avro/JSON messages; add `ingest_ts` + `kafka_offset`.
- Write to `raw_user_events` (Delta Lake, partitioned by `event_date`). No transformation - append-only.
- Kafka consumer group sized for 2,500 events/min burst peak.

### 7.2 Silver Transformation Pipelines

**Offline Articles (PySpark, trigger: 11:00 ICT daily)**
- Incremental: process only partitions where `ingest_ts > last_run_ts`.
- Schema evolution fix: add `relevant_article = NULL` for pre-2021 records if column absent.
- Normalize `tags`: lowercase, deduplicate values, store as JSON array string.
- Dedup by `article_id` (keep row with latest `ingest_ts`).
- Flag near-duplicates (`is_near_duplicate = TRUE`) by Jaccard similarity > 0.85 on title tokens - do not drop.
- Write to `stg_news_articles` (Delta Lake, partitioned by `published_date`).

**Streaming Events (Flink, continuous)**
- Assign watermarks: 45-minute allowed lateness on `event_timestamp`.
- Dedup by `event_id` within a 5-minute keyed state window (handles 2% re-emissions).
- Parse event-type-specific fields using `event_type` discriminator.
- Bad records (missing required fields, parse errors) --> dead-letter table `raw_user_events_dlq`.
- Write clean events to `stg_user_events` (Delta Lake, partitioned by `event_date`).

### 7.3 Gold Modeling Pipelines

**Dimensions (PySpark, daily 12:00 ICT)**
- `dim_article`: SCD1 upsert from `stg_news_articles`, merge on `article_id`; set `source_key` via join to `dim_source`.
- `dim_user`: SCD1 upsert from `stg_users`, merge on `user_id`.
- `dim_source`: full refresh (3 rows; negligible cost).
- Static dims (`dim_date`, `dim_event_type`, `dim_device_type`): pre-populated, no pipeline needed.
**`fact_article_daily_stats` (PySpark, every 30 min)**
- Read new `stg_user_events` since last run.
- Aggregate view/reaction/comment/share counts per (`article_id`, `event_date`).
- Join to `dim_article` and `dim_date` to resolve surrogate keys.
- `MERGE INTO` on (`article_key`, `event_date_key`).
**`obt_article_performance` (PySpark, every 30 min)**
- Join `fact_article_daily_stats` + `dim_article` + `dim_source` + `dim_date`.
- Compute `article_age_days` and `engagement_score`.
- `MERGE INTO` on (`article_id`, `event_date`).
All Gold writes use `MERGE INTO ... WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT` for idempotency.

### 7.4 Feature Pipelines

**`feat_article_offline` (PySpark, 2× daily at 11:30 + 22:00 ICT)**
- Read `stg_news_articles`; compute `f_article_age_days`, `f_article_tag_count`, `f_article_category_primary`, `f_article_has_relevant_link`.
- Set `event_timestamp = CURRENT_TIMESTAMP`, `created_ts = CURRENT_TIMESTAMP`.
- `MERGE INTO feat_article_offline` by (`article_id`, `event_timestamp`), keep latest `created_ts`.
**`feat_article_stream` (Flink, continuous)**
- Rolling window aggregations (1h views, 24h reactions/comments/shares, burst flag) from `stg_user_events`.
- Write snapshot to `feat_article_stream` every 5 minutes via Delta merge.

### 7.5 Pipeline Update Strategy

| Layer | Strategy |
|-------|----------|
| Bronze | Append-only; raw data never overwritten |
| Silver | Incremental by `ingest_ts`; idempotent re-runs produce identical output |
| Gold dims | SCD1 upsert on business key |
| Gold fact / OBT | Incremental `MERGE INTO` on composite key |
| Features | Rolling recompute + merge; keep latest `created_ts` per (`article_id`, `event_timestamp`) |
| Backfill | No backfill by default; max 1-day re-run with idempotent writes if needed |
| Late data | 45-min Flink watermark; post-watermark events go to side-output, reconciled on next run |

### 7.6 Pipeline Controls and Monitoring

**Quality gates per run:** schema check, BK uniqueness (Silver + Gold), null check on required keys, referential integrity (fact FKs --> dims), volume check (±50% of 7-day moving average).
 
**Run metadata** stored in `pipeline_run_log`: `run_id`, `pipeline_name`, `start_ts`, `end_ts`, `status`, `input_rows`, `output_rows`, `error_summary`.
 
**Recovery:** exponential backoff retry (max 3×, 5-min initial delay); bad records quarantined to `raw_*_dlq`; manual rerun via explicit `batch_id` + `date_range` parameters.
 
**Lineage:** publish Bronze --> Silver --> Gold --> Feature lineage to DataHub per run. At minimum, one lineage screenshot or export covering the core `raw_news_articles --> stg_news_articles --> dim_article / fact_article_daily_stats --> obt_article_performance` chain is required as evidence.


## 8. Warehouse Optimization

### 8.1 Partitioning Strategy

| Table | Partition Key | Z-ORDER (Delta Lake) |
|-------|--------------|----------------------|
| `raw_news_articles` | `published_date` | - |
| `stg_news_articles` | `published_date` | (`source_id`, `published_date`) |
| `raw_user_events` | `event_date` | - |
| `stg_user_events` | `event_date` | (`article_id`, `event_date`) |
| `fact_article_daily_stats` | `event_date_key` | (`article_key`, `event_date_key`) |
| `obt_article_performance` | `event_date` | (`event_date`, `source_name`, `category_primary`) |

### 8.2 Optimization Impact Example

**Workload:** daily trending articles dashboard - top 20 articles by `engagement_score` over 7 days, filtered by `source_name`.
 
**Bottleneck (before):** full table scan across all historical partitions; estimated ~38s runtime scanning ~8 GB (target, to be validated after implementation).
 
**Optimizations applied:**
- Partition `obt_article_performance` by `event_date` --> query scans only 7 daily partitions.
- Z-ORDER BY (`event_date`, `source_name`) --> file skipping within partitions.
- `engagement_score` materialized in OBT --> no recomputation per query.
**Estimated result:** runtime ~6s; data scanned reduced ~85%. *(To be measured and reported after implementation.)*
 
**Trade-off:** slightly higher write cost per merge due to Z-ORDER rewrite; acceptable for a read-heavy workload.

### 8.5 Maintenance Operations

| Operation | Frequency | Purpose |
|-----------|-----------|---------|
| `VACUUM` | Weekly | Remove obsolete Delta Lake files; reduce storage cost |
| `OPTIMIZE` | After each major load | Compact small files from streaming micro-batches |

## 9. Deliverables

1. **This design document** (`h02_schema_design.md`)
2. **Bronze pipeline code** - PySpark batch ingest for offline articles/users; Flink job for streaming events
3. **Silver pipeline code** - PySpark transform + dedup for articles; Flink streaming clean + watermark + DLQ job
4. **Gold pipeline code** - PySpark dimension, fact, and OBT scripts
5. **Feature pipeline code** - `feat_article_offline` (PySpark) and `feat_article_stream` (Flink)
6. **Lineage evidence** - DataHub lineage screenshot or export for the core article pipeline chain
7. **Warehouse optimization report** - before/after metrics for the dashboard query benchmark
