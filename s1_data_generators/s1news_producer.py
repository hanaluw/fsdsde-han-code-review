import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from s0config_schema_kafka import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, SCHEMA_REGISTRY_URL,
    DLQ_TOPIC, MAX_RETRY, NEWS_SCHEMA_STR
)


def create_producer():
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry_client, NEWS_SCHEMA_STR)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 50,
        "batch.num.messages": 1000,
        "acks": "all",
        "retries": 5,
        "enable.idempotence": True
    })
    return producer, avro_serializer


def delivery_report(err, msg):
    if err:
        print(f"[ERR] delivery failed: {err}")


def send_dlq(producer, record):
    producer.produce(DLQ_TOPIC, json.dumps(record, ensure_ascii=False).encode("utf-8"))


def send_with_retry(producer, avro_serializer, record, attempt=0):
    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            value=avro_serializer(
                record,
                SerializationContext(KAFKA_TOPIC, MessageField.VALUE)
            ),
            on_delivery=delivery_report
        )
    except Exception:
        if attempt < MAX_RETRY:
            send_with_retry(producer, avro_serializer, record, attempt + 1)
        else:
            send_dlq(producer, record)