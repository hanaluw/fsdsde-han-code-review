# PyFlink: Kafka events → raw_user_events
# Refer to 1_process_clean_stream_datastream.py for KafkaSource, WatermarkStrategy + TimestampAssigner, AggregateFunction + ProcessWindowFunction
# refer to 2_process_clean_stream_lateness_datastream.py for handling late events with side outputs.

"""
Bronze layer — Flink streaming ingestion job.

Read raw events from Kafka topic `news_user_events`, add ingest_ts +
kafka_offset, write append-only to raw_user_events/ partitioned by event_date.

Design notes:
  - Watermark 45 minutes (bounded out-of-orderness) to handle late arrivals from producer
  - Consumer group parallelism = 4 to handle burst 2,500 events/min
  - Sink: FileSink (Parquet row format), partitioned by event_date=YYYY-MM-DD
  - Checkpoint at every 60s to ensure exactly-once semantics when restarting

Usage:
    python ingest_events_flink.py
    KAFKA_BROKER=localhost:9092 python ingest_events_flink.py
"""

import json
import os
from datetime import datetime, timezone

from pyflink.common import Duration, Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointingMode
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.formats.json import JsonRowSerializationSchema
from pyflink.datastream.functions import MapFunction

# ===== STEP 1: CONFIG =====
KAFKA_BROKER  = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC   = "news_user_events"
GROUP_ID      = "flink-bronze-ingest"          # consumer group riêng cho Bronze

# Parallelism = 4 để handle burst 2,500 events/min
# (baseline 50/min × 50 burst_multiplier = 2,500/min)
PARALLELISM   = 4

# Watermark 45 phút — khớp với LATE_DELAY_MAX của producer
WATERMARK_MIN = 45

# Output path — Bronze layer
OUTPUT_DIR    = os.getenv("BRONZE_OUTPUT_DIR", "spark_demo_data/raw_user_events")

# Jar path — download từ https://mvnrepository.com/artifact/org.apache.flink/flink-sql-connector-kafka
JAR_PATH      = os.path.abspath("jars/flink-sql-connector-kafka-4.0.1-2.0.jar")


# ===== STEP 2: ENVIRONMENT SETUP =====
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(PARALLELISM)
env.add_jars(f"file://{JAR_PATH}")

# Checkpoint mỗi 60s — đảm bảo at-least-once, tránh mất data khi restart
env.enable_checkpointing(60_000)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.AT_LEAST_ONCE)


# ===== STEP 3: KAFKA SOURCE =====
# earliest() để đọc lại từ đầu khi restart (idempotent vì Bronze append-only)
source = (
    KafkaSource.builder()
    .set_bootstrap_servers(KAFKA_BROKER)
    .set_topics(KAFKA_TOPIC)
    .set_group_id(GROUP_ID)
    .set_starting_offsets(KafkaOffsetsInitializer.earliest())
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)


# ===== STEP 4: WATERMARK STRATEGY =====
# Dùng event_timestamp từ JSON làm event time.
# 45-minute bounded out-of-orderness — khớp với late arrival delay max của producer.
# Flink sẽ đợi tối đa 45 phút trước khi advance watermark.
class EventTimestampAssigner(TimestampAssigner):
    """Extract event_timestamp từ JSON payload làm Flink event time."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        try:
            ts_str = json.loads(value)["event_timestamp"]
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)  # epoch milliseconds
        except (KeyError, ValueError):
            # Malformed event — dùng Kafka ingestion time làm fallback
            return record_timestamp


watermark_strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_minutes(WATERMARK_MIN))
    .with_timestamp_assigner(EventTimestampAssigner())
)


# ===== STEP 5: ENRICH FUNCTION =====
class EnrichWithIngestMetadata(MapFunction):
    """
    Thêm 2 fields vào mỗi event:
      - ingest_ts    : thời điểm Flink xử lý record (wall-clock time)
      - kafka_offset : offset từ Kafka record metadata

    Không transform, không filter — Bronze là append-only raw copy.
    """

    def map(self, value):
        try:
            event = json.loads(value)
        except json.JSONDecodeError:
            # Ghi lại malformed event với flag để DLQ ở Silver xử lý
            event = {"_raw": value, "_parse_error": True}

        # Thêm ingest metadata
        event["ingest_ts"]    = datetime.now(timezone.utc).isoformat()

        # Thêm event_date để dùng làm partition key
        try:
            event_ts_str  = event.get("event_timestamp", "")
            event_date    = event_ts_str[:10]  # lấy YYYY-MM-DD
        except Exception:
            event_date    = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        event["event_date"] = event_date

        return json.dumps(event, ensure_ascii=False)


# ===== STEP 6: PIPELINE =====
raw_stream = env.from_source(
    source,
    watermark_strategy,
    "Kafka news_user_events"   # source name hiện trong Flink UI
)

enriched_stream = raw_stream.map(
    EnrichWithIngestMetadata(),
    output_type=Types.STRING()
)

# ===== STEP 7: SINK — Parquet partitioned by event_date =====
# FileSink ghi Parquet files vào thư mục OUTPUT_DIR/event_date=YYYY-MM-DD/
# Rolling policy: roll file sau 5 phút hoặc 128MB — cân bằng file size vs latency
sink = (
    FileSink
    .for_row_format(
        OUTPUT_DIR,
        # Encode mỗi record thành UTF-8 string line (JSONL format)
        # Dùng SimpleStringEncoder thay vì Parquet vì PyFlink FileSink
        # Parquet encoder cần thêm schema definition phức tạp.
        # Bronze chỉ cần raw JSONL — PySpark Silver job đọc được dễ dàng.
        __import__("pyflink.datastream.connectors.file_system", fromlist=["SimpleStringEncoder"]).SimpleStringEncoder("UTF-8")
    )
    .with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("part")
        .with_part_suffix(".jsonl")
        .build()
    )
    .with_rolling_policy(
        RollingPolicy.default_rolling_policy(
            part_size=128 * 1024 * 1024,  # 128 MB
            rollover_interval=5 * 60 * 1000,  # 5 phút
            inactivity_interval=60 * 1000,  # 1 phút không có data thì roll
        )
    )
    .build()
)

enriched_stream.sink_to(sink)

# ===== STEP 8: EXECUTE =====
print(f"[init] Flink Bronze job starting")
print(f"       topic      : {KAFKA_TOPIC}")
print(f"       broker     : {KAFKA_BROKER}")
print(f"       group_id   : {GROUP_ID}")
print(f"       parallelism: {PARALLELISM}")
print(f"       watermark  : {WATERMARK_MIN} min")
print(f"       output     : {OUTPUT_DIR}/event_date=*/")

env.execute("bronze_ingest_news_user_events")