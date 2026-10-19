"""
Streaming Job
=============

Spark Structured Streaming job that reads weather alerts from Kafka
and writes them to Postgres. Three queries run off one topic:

1. raw_alerts: every valid alert is upserted into raw_alerts,
   bad messages go to dead_letter_events
2. tumbling_5m: 5 minute window counts into alert_window_agg
3. sliding_15m: 15 minute window counts (every 5 minutes) into
   alert_window_agg

It also writes a heartbeat row so the dashboard can tell if the job
is still alive.

Run with (from the repo root):
    python streaming/stream_alerts.py

Dependencies:
- PySpark
- psycopg2
"""

import os
import time
from datetime import datetime, timezone
from typing import Callable

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from parsing import parse_alerts  # kept in its own file so the tests can import it

# ── Config ───────────────────────────────────────────────────────────
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

WATERMARK = "10 minutes"  # how late an event can arrive and still count in its window
TRIGGER = "10 seconds"    # how often a micro-batch runs
PG_TRIES = 6              # attempts before giving up on Postgres

# ── SQL ──────────────────────────────────────────────────────────────
# If the alert already exists, overwrite it with the newest version
UPSERT_SQL = """
INSERT INTO raw_alerts (alert_id, event_type, severity, region, effective_at, expires_at, status, ingested_at)
VALUES %s
ON CONFLICT (alert_id) DO UPDATE SET
    event_type = EXCLUDED.event_type, severity = EXCLUDED.severity, region = EXCLUDED.region,
    effective_at = EXCLUDED.effective_at, expires_at = EXCLUDED.expires_at,
    status = EXCLUDED.status, ingested_at = EXCLUDED.ingested_at
"""

# The same Kafka message replayed only gets stored once
DEAD_LETTER_SQL = """
INSERT INTO dead_letter_events (raw, reason, kafka_partition, kafka_offset)
VALUES %s
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""

HEARTBEAT_SQL = """
    INSERT INTO pipeline_heartbeat (batch_id, events_processed, written_at)
    VALUES (%s, %s, now())
"""

# The per-batch heartbeat goes quiet when no alerts come in, which looks
# the same as a dead job. This writes an idle row (batch_id -1), but only
# if nothing was written in the last 30 seconds
IDLE_HEARTBEAT_SQL = """
    INSERT INTO pipeline_heartbeat (batch_id, events_processed, written_at)
    SELECT -1, 0, now()
    WHERE NOT EXISTS (
        SELECT 1 FROM pipeline_heartbeat WHERE written_at > now() - interval '30 seconds'
    )
"""

WINDOW_UPSERT_SQL = """
INSERT INTO alert_window_agg (window_type, window_start, window_end, region, severity, alert_count, computed_at)
VALUES %s
ON CONFLICT (window_type, window_start, region, severity) DO UPDATE SET
    window_end = EXCLUDED.window_end, alert_count = EXCLUDED.alert_count, computed_at = EXCLUDED.computed_at
"""

# ── Spark session and Kafka source ───────────────────────────────────
spark = (
    SparkSession.builder.master("local[*]")
    .appName("stormstream")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# On a fresh checkpoint this starts from the beginning of the topic.
# After that the checkpoint remembers where it stopped
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather-alerts")
    .option("startingOffsets", "earliest")
    .load()
)


# ── Postgres helpers ─────────────────────────────────────────────────
def pg_connect() -> psycopg2.extensions.connection:
    """Open a Postgres connection using env vars (defaults match the docker setup)."""

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "15432")),
        dbname=os.getenv("POSTGRES_DB", "stormstream"),
        user=os.getenv("POSTGRES_USER", "stormstream"),
        password=os.getenv("POSTGRES_PASSWORD", "stormstream"),
    )


def pg_run(work: Callable[[psycopg2.extensions.cursor], None]) -> None:
    """
    Run work(cur) as one transaction, retrying if Postgres can't be reached.

    Waits a little longer after each failed attempt and raises
    after PG_TRIES attempts.
    """

    for attempt in range(1, PG_TRIES + 1):
        conn = None
        try:
            conn = pg_connect()
            # Commits if it works, rolls back if it fails
            with conn, conn.cursor() as cur:
                work(cur)
            return
        except psycopg2.OperationalError as e:
            if attempt == PG_TRIES:
                raise
            wait = 2 * attempt
            print(f"!!! postgres problem (try {attempt}/{PG_TRIES}), retrying in {wait}s: {str(e).splitlines()[0]}")
            time.sleep(wait)
        finally:
            if conn:
                conn.close()


# ── Spark helpers ────────────────────────────────────────────────────
def iso_col(c: Column) -> Column:
    """
    Turn a timestamp column into text with an explicit +00:00.

    Without this PySpark hands Python a naive datetime in the laptop's
    timezone and everything shifts by 4 hours.
    """

    return F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'+00:00'")


def iso(name: str) -> Column:
    """Same as iso_col but takes a column name and keeps that name."""

    return iso_col(F.col(name)).alias(name)


# ── Query 1: raw_alerts and dead letters ─────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write one micro-batch: good alerts to raw_alerts, bad ones to
    dead_letter_events, plus one heartbeat row.
    """

    flat = parse_alerts(batch_df)

    # Good rows in offset order, timestamps already converted to text
    good_rows = (
        flat.filter(F.col("is_valid"))
        .select("offset", "alert_id", "event_type", "severity", "region",
                iso("effective_at"), iso("expires_at"), "status", iso("ingested_at"))
        .orderBy("offset")
        .collect()
    )
    bad_rows = flat.filter(~F.col("is_valid")).select("raw", "reason", "partition", "offset").collect()

    # If one alert shows up twice in a batch keep only the latest, because
    # Postgres won't let a single upsert touch the same row twice.
    # Same alert = same partition, so offset order is the real order
    latest = {}
    for r in good_rows:
        latest[r.alert_id] = r
    upserts = [(r.alert_id, r.event_type, r.severity, r.region, r.effective_at,
                r.expires_at, r.status, r.ingested_at) for r in latest.values()]
    dead = [(r.raw, r.reason, r.partition, r.offset) for r in bad_rows]

    def write(cur):
        if upserts:
            execute_values(cur, UPSERT_SQL, upserts)
        if dead:
            execute_values(cur, DEAD_LETTER_SQL, dead)
        # One heartbeat row per batch: how many messages it read (good and bad)
        cur.execute(HEARTBEAT_SQL, (batch_id, len(good_rows) + len(bad_rows)))

    pg_run(write)

    print(f"--- raw_alerts batch {batch_id}: wrote {len(upserts)} alerts, {len(dead)} dead letters")


# ── Queries 2 and 3: window counts ───────────────────────────────────
# Only real, usable alerts count. Expired events are left out on purpose,
# so alert_count means "alerts that showed up or changed in this window".
# The dedupe drops the same alert delivered twice by Kafka
valid_alerts = (
    parse_alerts(raw)
    .filter(F.col("is_valid") & F.col("status").isin("active", "updated"))
    .withWatermark("ingested_at", WATERMARK)
    .dropDuplicatesWithinWatermark(["alert_id", "status", "severity", "expires_at"])
)


def make_window_writer(window_type: str) -> Callable[[DataFrame, int], None]:
    """
    Build the batch writer for one kind of window.

    Returns:
    --------
    function(batch_df, batch_id) that upserts the window counts
    """

    def write(batch_df: DataFrame, batch_id: int) -> None:
        # Update mode: only windows whose count changed show up here,
        # with the total so far
        now = datetime.now(timezone.utc).isoformat()
        rows = [(window_type, r.window_start, r.window_end, r.region, r.severity, r.alert_count, now)
                for r in batch_df.collect()]
        if not rows:
            return
        pg_run(lambda cur: execute_values(cur, WINDOW_UPSERT_SQL, rows))
        print(f"--- {window_type} batch {batch_id}: upserted {len(rows)} window rows")

    return write


def start_window_query(window_type: str, checkpoint: str, *window_args: str) -> StreamingQuery:
    """
    Start a windowed count of alerts per region and severity.

    window_args are the arguments for F.window after the time column:
    ("5 minutes") is tumbling, ("15 minutes", "5 minutes") is sliding.
    """

    counts = valid_alerts.groupBy(F.window("ingested_at", *window_args), "region", "severity").count()
    out = counts.select(
        iso_col(F.col("window.start")).alias("window_start"),
        iso_col(F.col("window.end")).alias("window_end"),
        "region", "severity",
        F.col("count").cast("int").alias("alert_count"),
    )
    return (
        out.writeStream.queryName(window_type)
        .outputMode("update")
        .foreachBatch(make_window_writer(window_type))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER)
        .start()
    )


# ── Start everything ─────────────────────────────────────────────────
(
    raw.writeStream.queryName("raw_alerts")
    .foreachBatch(process_batch)
    .option("checkpointLocation", "checkpoints/raw_alerts")
    .trigger(processingTime=TRIGGER)
    .start()
)
start_window_query("tumbling_5m", "checkpoints/tumbling_5m", "5 minutes")
start_window_query("sliding_15m", "checkpoints/sliding_15m", "15 minutes", "5 minutes")


def write_idle_heartbeat() -> None:
    """Write the idle heartbeat row (only if the last 30 seconds were quiet)."""

    pg_run(lambda cur: cur.execute(IDLE_HEARTBEAT_SQL))


# awaitAnyTermination raises if a query crashes, so a dead job stops
# heartbeating. False means 15 seconds passed and nothing died
while not spark.streams.awaitAnyTermination(15):
    write_idle_heartbeat()
