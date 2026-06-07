# Fintech Intelligent Platform - Luu Gia Han's coursework proposal in EDAI course

## 0. Business Problem
Financial innovation in Vietnam is fragmented across hundreds of news articles, press releases, and regulatory announcements.

Analysts, researchers, and investors struggle to answer questions such as:
- Which banks are adopting AI most aggressively?
- Which fintech companies are expanding their partnerships?
- How is Open Banking evolving in Vietnam?
- How does the fintech ecosystem change over time?

The platform centralizes and structures these signals automatically.

## 1. Objectives
This project builds a data platform for ingesting and managing Vietnamese fintech and banking news content at scale. The platform centralizes raw news articles from multiple sources alongside synthetic reader engagement signals, establishing a clean data foundation. Over time, this foundation will be extended with NLP-based entity extraction and relationship modeling to evolve into a fintech intelligence and knowledge platform.

The generator produces two data paths:
- **Offline**: Raw news articles crawled from Vietnamese financial news websites (cafef, VnEconomy, VJST).
- **Streaming**: Synthetic user interaction events simulating reader engagement with articles.

## 2. Offline Dataset Design
### 2.1. Offline tables

| Table | Grain | Key Columns |
|-------|-------|------------|
| `news_articles` | one per article | `article_id`, `url`, `title`, `source_id`, `published_datetime`, `snippet`, `tags`, `relevant_article` |
| `news_sources` | one per domain | `source_id`, `source_name`, `domain_name` |
| `users` | one per user | `user_id`, `signup_ts`, `email_subscription`|

article_id = sha256(url), the urls (lowercase, strip UTM params) will be normalized before hasing into ids

### 2.1. Data source
News articles are crawled from Vietnamese financial news websites:
| Source | Domain | Coverage |
|--------|--------|----------|
| CafeF | cafef.vn | Digital economy, Smart Money |
<!-- | VnEconomy | vneconomy.vn | Digital economy|
| Vietnam Journal of Science and Technology | vjst.vn | Innovation, Digital transformation | -->

**Crawl scope:** articles published from 2018 to present, filtered by fintech/digital economy categories.
 
**Crawl frequency:** daily batch, triggered at 10:00 ICT.

### 2.3. Offline Data Problems
**Compulsory:**
- **Skew**: 75% articles from Digital economy, 25% articles from Smart Money
- **High cardinality**: distinct values in `tags`
- **Schema evolution**: articles crawled before 2021 having no `relevant_article`

**Output:** Parquet, partitioned by `published_date` (daily partition)

---

## 3. Streaming Dataset Design

### 3.1 Event Stream Schema

User interaction events are **fully synthetic**, designed to simulate realistic reader behavior on a fintech news platform.

Single unified Kafka topic `fintech_user_events` with `event_type` discriminator field.
 
**Core fields (all events):**
- `event_id` - UUID v4
- `event_type` - `article_view` | `reaction` | `comment_post` | `share`
- `event_timestamp` - when the event occurred (reader's local time)
- `created_ts` - when the event was written to the stream (ingest time)
- `user_id` - synthetic reader ID
- `session_id` - groups events within one browsing session
- `article_id` - FK to `news_articles`
- `device_type` - `mobile` | `desktop` | `tablet`
- `source_referrer` - `direct` | `facebook` | `zalo` | `google` | `email`
**Event-specific fields:**
 
| event_type | Additional Fields |
|------------|------------------|
| `article_view` | `read_duration_seconds` (nullable), `scroll_depth_pct` (0-100) |
| `reaction` | `reaction_type` (`like` \| `insightful` \| `concerned`) |
| `comment_post` | `comment_text`, `comment_length` |
| `share` | `share_platform` (`facebook` \| `zalo` \| `copy_link`) |

### 3.2 Streaming Data Problems

**Compulsory:**
- **Bursts**: baseline 50 events/min → spikes to 2500 events/min for 15-minute windows triggered by major news events. Two burst windows per day: 08:00-08:15 and 12:00-12:15 ICT.
- **Late arrivals**: 20% of events have `created_ts` delayed 5-45 minutes after `event_timestamp` (mobile app offline sync, weak network conditions).
**Duplicate**: 2% duplicate events - same `event_id` re-emitted within 1-5 minutes (mobile client retry on failed acknowledgment).

**Output:** Avro/JSON one event per message

### 3.3 Realistic Behavioral Patterns
 
To make synthetic data meaningful for downstream features:
 
- `article_view` events follow article age decay: articles receive 80% of their views within 24 hours of publication.
- `reaction` and `comment_post` rates are higher for articles tagged `technology` and `ai` categories.
- `share` events skew heavily toward Facebook (65%) - consistent with Vietnamese social media usage.
- User sessions average 2.3 article views, with 8% of sessions producing at least one comment.

---

## 4. Feature Engineering
 
Features to be computed once Bronze/Silver pipelines are stable.
 
**Offline article features (computed 2 times daily):**
- `f_article_age_days` - days since published
- `f_article_tag_count` - content length proxy for depth
- `f_article_category_primary` - top category label
**Streaming engagement features (rolling windows):**
- `f_article_views_1h` - view count in last 1 hour
- `f_article_reactions_24h` - reactions in last 24 hours
- `f_article_comment_count_24h` - comments in last 24 hours
- `f_article_share_rate_24h` - shares / views ratio
- `f_burst_activity_flag` - 1 if event occurs during a detected burst window

<!-- **Unified article feature table - to be update** (grain: `article_id`, `event_timestamp`):
- join offline article features + streaming engagement features
- refreshed every 15 minutes
- used for LLM retrieval ranking and downstream NLP prioritization -->

---

## 5. Generator Configuration

```yaml
# Offline crawl settings
crawl_sources:
  - cafef.vn
crawl_start_date: "2018-01-01"
crawl_schedule: "10:00 ICT daily"
skew_source_ratio:
  cafef.vn: 0.50
  vneconomy.vn: 0.25
  vjst.vn: 0.25
duplicate_article_rate: 0.03        # same setting across sources
`article_id`= sha256(url)
schema_change_cutoff_year: 2021     # articles before this missing `related_articles`

# Streaming generator settings
n_synthetic_users: 50000
daily_active_user_rate: 0.05~0.15   # 5-15% of users active per day
email_subscription:
    yes: 0.7
    no: 0.3
daily_events_range: [2000, 8000]   # right-skewed bell curve, median ~3500
base_events_per_min: 50
burst_multiplier: 50                # 50x baseline = ~2500 events/min
burst_windows:
  - "08:00-08:15"
  - "12:00-12:15"
late_arrival_rate: 0.20
late_delay_min_max: [5, 45]
duplicate_rate_stream: 0.02
article_view_decay_24h: 0.80        # 80% of views happen in first 24h
facebook_share_ratio: 0.65
high_engagement_tags: ["công nghệ", "trí tuệ nhân tạo", "ai"]  # higher reaction/comment rate
comment_templates: [
    # positive comments
    `Bài viết rất hay, cung cấp nhiều thông tin hữu ích!`,
    `Cảm ơn tác giả đã chia sẻ góc nhìn sâu sắc này.`,
    `Phân tích rất chi tiết, tôi học được nhiều điều mới.`,
    `Thông tin phù hợp với xu hướng hiện tại của trường.`,
    `Bài báo giúp tôi hiểu rõ hơn về fintech Việt Nam.`,
    `Tác giả có cái nhìn toàn diện về lĩnh vực ngân hàng số.`,
    `Rất đáng đọc, tôi sẽ chia sẻ cho đồng nghiệp ngay.`,
    `Số liệu trong bài rất thuyết phục và có nguồn gốc rõ ràng.`,
    `Bài viết mở ra nhiều góc nhìn mới!`,
    `Tôi đồng ý hoàn thành việc nhận định nghĩa của tác giả.`,
    # negative comments
    `Bài viết quá chung chung, thiếu số liệu cụ thể thuyết phục.`,
    `Tác giả có vẻ không hiểu rõ về lĩnh vực này lắm.`,
    `Thông tin đã cũ, không còn phù hợp với thực tế nữa.`,
    `Bài báo thiên vị rõ ràng, thiếu tính khách quan trung lập.`,
    `Phân tích nông, chỉ nói lại những gì ai cũng biết rồi.`,
    `Tiêu đề câu view nhưng nội dung không có gì mới.`,
    `Tôi không đồng ý với kết luận của bài viết này.`,
    `Bài viết copy từ nguồn khác, không có giá trị gì thêm.`,
    `Thiếu góc nhìn từ phía người dùng thực tế sử dụng.`,
    `Bài này chỉ quảng cáo cho doanh nghiệp, không khách quan.`,
]             
random_seed: 42

Dedup keys: `article_id` + `published_date` (offline), `event_id` (streaming).
```

## 6. Deliverables

1. **Crawler code**
2. **Streaming generator code**
3. **Quality report**
   - Source distribution (article count per source)
   - Schema evolution evidence (null rate in `tags`, `relevant_article` by partition age)
   - Duplicate article detection (title similarity rate across sources)
   - Streaming burst profile (events/min time series)
   - Late arrival rate (% events where `created_ts - event_timestamp > threshold` [5, 45])
   - Duplicate event rate before/after dedup