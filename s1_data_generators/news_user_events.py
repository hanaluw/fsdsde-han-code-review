"""
Produces a continuous stream of fintech user events to Kafka topic `fintech_user_events`.

Each event is emitted individually to Kafka with a real-time delay (1 event/second by default), but bursts are possible (50 events/min → 2500 events/min during peak hours), late arrivals (20% of event_timestamps are 5-45 minutes later than created_ts), and duplicates (2% of events re-emit with the same event_id after 1-5 minutes).

However, the schema and data problems of news_user_events.py are used.

Problems injected (like batch generator): 
1. Bursts — 50 events/min baseline → 2500/min at 08:00-08:15 & 12:00-12:15 ICT 
2. Late arrivals — 20% of events have event_timestamp 5-45 minutes later than created_ts 
3. Duplicates — 2% events re-emit with event_id in 1-5 minutes

Usage: 
python produce_user_events.py # 1 event/s, real-time ICT 
python produce_user_events.py --rate 5 # 5 events/s 
python produce_user_events.py --broker localhost:9092 
python produce_user_events.py --limit 1000 # stop after 1000 events
"""

import argparse
import json
import random
import time
import uuid
import os
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

# ===== STEP 1: CONFIG =====
TOPIC        = "fintech_user_events"
ARTICLE_IDS_FILE = os.path.join("spark_demo_data", "article_ids.json")

N_USERS      = 50_000
ALL_USER_IDS = [f"user_{i:06d}" for i in range(1, N_USERS + 1)]

# Burst windows theo giờ ICT (UTC+7)
BURST_WINDOWS = [("08:00", "08:15"), ("12:00", "12:15")]
BURST_MULTIPLIER    = 50
BASE_EVENTS_PER_MIN = 50

LATE_ARRIVAL_RATE = 0.20
LATE_DELAY_MIN    = 5    # phút
LATE_DELAY_MAX    = 45   # phút
DUPLICATE_RATE    = 0.02

FACEBOOK_SHARE_RATIO = 0.65
ARTICLE_VIEW_DECAY_24H = 0.80

DEVICE_TYPES     = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS   = [0.60, 0.30, 0.10]
REFERRERS        = ["direct", "facebook", "zalo", "google", "email"]
REFERRER_WEIGHTS = [0.20, 0.35, 0.20, 0.15, 0.10]
REACTIONS        = ["like", "insightful", "concerned"]
SHARE_PLATFORMS  = ["facebook", "zalo", "copy_link"]

COMMENT_TEMPLATES = [
    # positive
    "Bài viết rất hay, cung cấp nhiều thông tin hữu ích!",
    "Cảm ơn tác giả đã chia sẻ góc nhìn sâu sắc này.",
    "Phân tích rất chi tiết, tôi học được nhiều điều mới.",
    "Thông tin phù hợp với xu hướng hiện tại của thị trường.",
    "Bài báo giúp tôi hiểu rõ hơn về fintech Việt Nam.",
    "Tác giả có cái nhìn toàn diện về lĩnh vực ngân hàng số.",
    "Rất đáng đọc, tôi sẽ chia sẻ cho đồng nghiệp ngay.",
    "Số liệu trong bài rất thuyết phục và có nguồn gốc rõ ràng.",
    "Bài viết mở ra nhiều góc nhìn mới!",
    "Tôi đồng ý với nhận định của tác giả.",
    # negative
    "Bài viết quá chung chung, thiếu số liệu cụ thể thuyết phục.",
    "Tác giả có vẻ không hiểu rõ về lĩnh vực này lắm.",
    "Thông tin đã cũ, không còn phù hợp với thực tế nữa.",
    "Bài báo thiên vị rõ ràng, thiếu tính khách quan trung lập.",
    "Phân tích nông, chỉ nói lại những gì ai cũng biết rồi.",
    "Tiêu đề câu view nhưng nội dung không có gì mới.",
    "Tôi không đồng ý với kết luận của bài viết này.",
    "Bài viết copy từ nguồn khác, không có giá trị gì thêm.",
    "Thiếu góc nhìn từ phía người dùng thực tế sử dụng.",
    "Bài này chỉ quảng cáo cho doanh nghiệp, không khách quan.",
]


# ===== STEP 2: LOAD ARTICLE IDS =====
def load_article_ids() -> tuple[list[str], list[str]]:
    """Load article IDs, chia sẵn old/recent cho age-decay."""
    try:
        with open(ARTICLE_IDS_FILE, encoding="utf-8") as f:
            ids = json.load(f)
        print(f"[init] {len(ids):,} article_ids từ {ARTICLE_IDS_FILE}")
    except FileNotFoundError:
        print(f"[WARN] {ARTICLE_IDS_FILE} not found — dùng fallback synthetic IDs")
        ids = [f"art_{i:05d}" for i in range(500)]

    cutoff     = max(1, len(ids) * 2 // 3)
    old_ids    = ids[:cutoff]     # 2/3 đầu = old articles
    recent_ids = ids[cutoff:]     # 1/3 cuối = recent articles
    return old_ids, recent_ids


# ===== STEP 3: HELPERS =====
def _in_burst(ts: datetime) -> bool:
    """Kiểm tra ts có rơi vào burst window (theo ICT) không."""
    ict_hm = (ts + timedelta(hours=7)).strftime("%H:%M")
    return any(start <= ict_hm < end for start, end in BURST_WINDOWS)


def _sample_article_id(old_ids: list[str], recent_ids: list[str]) -> str:
    """Age-decay: 80% chọn từ recent, 20% từ old."""
    if recent_ids and random.random() < ARTICLE_VIEW_DECAY_24H:
        return random.choice(recent_ids)
    return random.choice(old_ids) if old_ids else random.choice(recent_ids)


# ===== STEP 4: BUILD EVENT =====
def make_event(user_id: str, session_id: str,
               old_ids: list[str], recent_ids: list[str]) -> dict:
    """Tạo một event với event_timestamp = now (real-time)."""
    now        = datetime.now(timezone.utc)
    article_id = _sample_article_id(old_ids, recent_ids)

    # --- high_engagement_tags: 30% articles assumed to be high-engagement ---
    # Producer không có metadata tag → dùng xác suất để simulate:
    # 30% chance article is tagged công nghệ/AI → boost reaction + comment weight
    is_high_engagement = random.random() < 0.30
    if is_high_engagement:
        event_type = random.choices(
            ["article_view", "reaction", "comment_post", "share"],
            weights=[0.50, 0.28, 0.15, 0.07]  # reaction + comment cao hơn
        )[0]
    else:
        event_type = random.choices(
            ["article_view", "reaction", "comment_post", "share"],
            weights=[0.65, 0.18, 0.10, 0.07]  # baseline weights
        )[0]

    # --- late arrival: created_ts delayed 5-45 min AFTER event_timestamp ---
    # Simulate mobile app offline sync / weak network:
    # event happened at event_ts, but only written to Kafka (created_ts) much later.
    event_ts = now
    if random.random() < LATE_ARRIVAL_RATE:
        delay_min  = random.randint(LATE_DELAY_MIN, LATE_DELAY_MAX)
        created_ts = now + timedelta(minutes=delay_min)  # created_ts trễ hơn event_ts
    else:
        created_ts = now  # on-time: written to Kafka immediately

    event: dict = {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "event_timestamp": event_ts.isoformat(),
        "created_ts":      created_ts.isoformat(),
        "user_id":         user_id,
        "session_id":      session_id,
        "article_id":      article_id,
        "device_type":     random.choices(DEVICE_TYPES, DEVICE_WEIGHTS)[0],
        "source_referrer": random.choices(REFERRERS, REFERRER_WEIGHTS)[0],
    }

    # --- event-specific fields ---
    if event_type == "article_view":
        event["read_duration_seconds"] = (
            random.randint(10, 599) if random.random() < 0.85 else None
        )
        event["scroll_depth_pct"] = random.randint(0, 100)

    elif event_type == "reaction":
        event["reaction_type"] = random.choice(REACTIONS)

    elif event_type == "comment_post":
        text = random.choice(COMMENT_TEMPLATES)
        event["comment_text"]   = text
        event["comment_length"] = len(text)

    elif event_type == "share":
        event["share_platform"] = random.choices(
            SHARE_PLATFORMS,
            weights=[FACEBOOK_SHARE_RATIO,
                     (1 - FACEBOOK_SHARE_RATIO) * 0.6,
                     (1 - FACEBOOK_SHARE_RATIO) * 0.4]
        )[0]

    return event


# ===== STEP 5: EMIT =====
def emit(producer: Producer, event: dict, is_late: bool, is_dup: bool = False):
    """Produce một event lên Kafka và print log."""
    payload = json.dumps(event, ensure_ascii=False).encode("utf-8")
    producer.produce(TOPIC, value=payload)
    producer.poll(0)  # trigger delivery callback, tránh buffer đầy

    label = "DUP " if is_dup else ("LATE" if is_late else "OK  ")
    print(
        f"[{label}] {event['event_type']:<12} "
        f"user={event['user_id']}  "
        f"event_ts={event['event_timestamp'][:19]}  "
        f"created_ts={event['created_ts'][:19]}",
        flush=True
    )


# ===== STEP 6: MAIN LOOP =====
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate",   type=float, default=1.0,             help="Events per second")
    parser.add_argument("--limit",  type=int,   default=0,               help="Dừng sau N events (0 = vô hạn)")
    parser.add_argument("--broker", type=str,   default="localhost:9092", help="Kafka broker")
    args = parser.parse_args()

    old_ids, recent_ids = load_article_ids()
    producer  = Producer({"bootstrap.servers": args.broker})
    delay     = 3.0 / args.rate
    count     = 0

    # Session cache: mỗi user giữ 1 session_id trong suốt chương trình
    user_sessions: dict[str, str] = {}

    print(f"[init] Producing to topic '{TOPIC}' @ {args.broker}  rate={args.rate}/s\n")

    try:
        while True:
            now     = datetime.now(timezone.utc)
            user_id = random.choice(ALL_USER_IDS)
            session = user_sessions.setdefault(user_id, str(uuid.uuid4()))

            # --- burst: emit thêm nhiều event liên tiếp không delay ---
            in_burst = _in_burst(now)
            n_emit   = BURST_MULTIPLIER if in_burst else 1

            for _ in range(n_emit):
                is_late = random.random() < LATE_ARRIVAL_RATE
                event   = make_event(user_id, session, old_ids, recent_ids)
                emit(producer, event, is_late=is_late)
                count += 1

                # --- duplicate: re-emit cùng event_id sau 1-5 phút (giả lập) ---
                # Trong real-time producer, "delay 1-5 phút" = emit ngay với created_ts +delay
                if random.random() < DUPLICATE_RATE:
                    dup = dict(event)
                    dup_delay = random.randint(1, 5)
                    dup["created_ts"] = (
                        datetime.fromisoformat(event["created_ts"])
                        + timedelta(minutes=dup_delay)
                    ).isoformat()
                    emit(producer, dup, is_late=is_late, is_dup=True)
                    count += 1

                if args.limit and count >= args.limit:
                    return

            time.sleep(delay)

    finally:
        producer.flush()
        print(f"\n[done] Emitted {count:,} events total.")


if __name__ == "__main__":
    main()