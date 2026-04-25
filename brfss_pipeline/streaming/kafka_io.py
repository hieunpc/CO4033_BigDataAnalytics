from __future__ import annotations

import importlib
import json
from pathlib import Path

from brfss_pipeline.config import DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_CHUNK_SIZE, DEFAULT_TOPIC
from brfss_pipeline.data.loading import iter_brfss_records


def produce_to_kafka(
    csv_path: str | Path,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_TOPIC,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    try:
        kafka_module = importlib.import_module("kafka")
        KafkaProducer = kafka_module.KafkaProducer
    except ImportError as exc:
        raise ImportError("kafka-python is required. Install requirements-bigdata.txt first.") from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if isinstance(value, str) else value,
        retries=5,
        linger_ms=10,
    )

    try:
        for record in iter_brfss_records(csv_path, chunk_size=chunk_size):
            producer.send(topic, value=record)
        producer.flush()
    finally:
        producer.close()
