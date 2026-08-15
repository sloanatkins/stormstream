"""
StormStream Poller
==================

Polls the NWS alerts API, works out what changed since the last poll,
and sends only the changes to Kafka.

Flow each cycle:
1. fetch active alerts (real ones only, no tests or exercises)
2. clean each one, bad ones go to a dead letter file
3. diff against what was seen last time
4. send the changes to Kafka, then save the new seen state

Run from the repo root:
    python -m poller.main --once   (one cycle, for testing)
    python -m poller.main          (loops forever)

Settings (env vars or a .env file):
- NWS_POLL_INTERVAL_SECONDS (default 45)
- NWS_REGION_FILTER (default: all of the US)
- ALERTS_TOPIC (default weather-alerts)
- KAFKA_BOOTSTRAP_HOST (default localhost:9092)
- DEAD_LETTER_PATH (default poller/dead_letter.jsonl)
- SEEN_DB_PATH (default poller/seen.sqlite)

Dependencies:
- requests
- kafka-python
"""

import argparse
import json
import logging
import os
import time

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

from poller.diff import diff
from poller.nws import BadAlert, fetch_active, parse_feature
from poller.producer import make_producer, send_events
from poller.store import SeenStore

log = logging.getLogger("poller")


# ── Helpers ──────────────────────────────────────────────────────────
def load_env_file(path: str = ".env") -> None:
    """
    Load KEY=value lines from a .env file into the environment.

    Tiny loader so python-dotenv isn't needed. Real env vars win
    over the file.
    """

    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def dead_letter(path: str, reason: str, raw: dict) -> None:
    """Append a bad alert and why it was rejected to the dead letter file."""

    with open(path, "a") as f:
        f.write(json.dumps({"reason": reason, "raw": raw}) + "\n")


# ── One poll cycle ───────────────────────────────────────────────────
def run_cycle(producer: KafkaProducer, store: SeenStore, topic: str,
              area: str | None, dead_letter_path: str) -> None:
    """Fetch, clean, diff and send once. Raises if the API or Kafka fails."""

    features = fetch_active(area=area)
    # Skip tests and exercises, we only want real alerts
    features = [f for f in features if (f.get("properties") or {}).get("status") == "Actual"]

    events = []
    bad = 0
    for feature in features:
        try:
            events.append(parse_feature(feature))
        except BadAlert as e:
            bad += 1
            log.warning("skipping bad alert: %s", e)
            dead_letter(dead_letter_path, str(e), feature)

    seen = store.load()
    to_send, new_seen = diff(seen, events)
    send_events(producer, topic, to_send)

    # Only save AFTER the flush worked. If kafka was down we raised before
    # this line, so the same changes get sent again next cycle
    store.save(new_seen)
    log.info("polled %d alerts, sent %d changes, %d bad", len(features), len(to_send), bad)


# ── Main loop ────────────────────────────────────────────────────────
def main() -> None:
    """Read settings, then poll forever (or once with --once)."""

    load_env_file()
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="run one cycle and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("kafka").setLevel(logging.WARNING)  # the library is chatty at INFO

    interval = int(os.getenv("NWS_POLL_INTERVAL_SECONDS", "45"))
    area = os.getenv("NWS_REGION_FILTER") or None
    topic = os.getenv("ALERTS_TOPIC", "weather-alerts")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:9092")
    dl_path = os.getenv("DEAD_LETTER_PATH", "poller/dead_letter.jsonl")

    producer = make_producer(bootstrap)
    store = SeenStore(os.getenv("SEEN_DB_PATH", "poller/seen.sqlite"))
    log.info("starting: topic=%s area=%s interval=%ss", topic, area or "ALL", interval)

    failures = 0
    while True:
        try:
            run_cycle(producer, store, topic, area, dl_path)
            failures = 0
        except (requests.RequestException, KafkaError) as e:
            failures += 1
            log.error("cycle failed (%d in a row): %s", failures, e)

        if args.once:
            break
        # Wait longer after each failure in a row: 2x, 4x, 8x... capped at 10 min
        wait = min(interval * 2 ** failures, 600) if failures else interval
        time.sleep(wait)

    producer.close()
    store.close()


if __name__ == "__main__":
    main()
