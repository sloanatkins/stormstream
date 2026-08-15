"""
Kafka Producer
==============

Kafka side of the poller. Builds the producer and sends alert events
to the topic.

Messages are keyed by region, so all alerts for one region go to the
same partition and stay in order.

Dependencies:
- kafka-python
"""

import json

from kafka import KafkaProducer
from kafka.serializer import Serializer


# ── Serializers ──────────────────────────────────────────────────────
# kafka-python wants classes with a serialize method instead of lambdas,
# otherwise it prints a deprecation warning on every start
class KeySerializer(Serializer):
    """Region string -> bytes."""

    def serialize(self, topic: str, key: str) -> bytes:
        return key.encode("utf-8")


class ValueSerializer(Serializer):
    """Event dict -> JSON bytes."""

    def serialize(self, topic: str, value: dict) -> bytes:
        return json.dumps(value).encode("utf-8")


# ── Producer ─────────────────────────────────────────────────────────
def make_producer(bootstrap: str) -> KafkaProducer:
    """
    Build a producer that waits for Kafka to confirm every message.

    bootstrap is the broker address, like "localhost:9092".
    """

    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=KeySerializer(),
        value_serializer=ValueSerializer(),
        acks="all",   # wait for kafka to confirm, not just fire and forget
        retries=5,
    )


def send_events(producer: KafkaProducer, topic: str, events: list[dict]) -> None:
    """Send every event to the topic and wait until Kafka has them all."""

    for ev in events:
        # Key = region, so all alerts for one region land on the same partition
        producer.send(topic, key=ev["region"], value=ev)
    # Block until kafka has confirmed everything
    producer.flush(timeout=30)
