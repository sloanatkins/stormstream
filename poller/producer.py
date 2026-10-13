"""Kafka side of the poller."""
import json

from kafka import KafkaProducer


def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",   # wait for kafka to confirm, not just fire and forget
        retries=5,
    )


def send_events(producer, topic, events):
    for ev in events:
        # key = state, so all alerts for one state land on the same partition
        producer.send(topic, key=ev["region"], value=ev)
    # block until kafka has confirmed everything
    producer.flush(timeout=30)
