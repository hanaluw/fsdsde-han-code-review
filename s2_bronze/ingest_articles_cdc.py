# CDC consumer: reads Debezium change events from Kafka

import json
from confluent_kafka import Consumer

from constants import KAFKA_BOOTSTRAP_SERVERS, KAFKA_NEWS_TOPIC

consumer = Consumer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "cdc-consumer-group",
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([KAFKA_NEWS_TOPIC])
print(f"Listening for changes on topic: {KAFKA_NEWS_TOPIC}\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        event = json.loads(msg.value().decode("utf-8"))

        payload = event.get("payload", event)

        if payload is None:
            continue

finally:
    consumer.close()