"""Spark job: Kafka -> Postgres. Three streaming queries off one topic:
  1. every valid alert -> raw_alerts (upsert), bad ones -> dead_letter_events
  2. 5 minute tumbling window counts -> alert_window_agg
  3. 15 minute sliding window counts (every 5 min) -> alert_window_agg
"""
import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]
STATUSES = ["active", "updated", "expired"]
REQUIRED = ["alert_id", "event_type", "severity", "region", "effective_at", "status", "ingested_at"]
WATERMARK = "10 minutes"
TRIGGER = "10 seconds"

# what a good message looks like. everything is read as a string first and the
# timestamps are cast below, so a bad timestamp turns into null and gets caught
FIELDS = ["alert_id", "event_type", "severity", "region", "effective_at", "expires_at", "status", "ingested_at"]
ALERT_SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])

UPSERT_SQL = """
INSERT INTO raw_alerts (alert_id, event_type, severity, region, effective_at, expires_at, status, ingested_at)
VALUES %s
ON CONFLICT (alert_id) DO UPDATE SET
    event_type = EXCLUDED.event_type, severity = EXCLUDED.severity, region = EXCLUDED.region,
    effective_at = EXCLUDED.effective_at, expires_at = EXCLUDED.expires_at,
    status = EXCLUDED.status, ingested_at = EXCLUDED.ingested_at
"""

DEAD_LETTER_SQL = """
INSERT INTO dead_letter_events (raw, reason, kafka_partition, kafka_offset)
VALUES %s
ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
"""

WINDOW_UPSERT_SQL = """
INSERT INTO alert_window_agg (window_type, window_start, window_end, region, severity, alert_count, computed_at)
VALUES %s
ON CONFLICT (window_type, window_start, region, severity) DO UPDATE SET
    window_end = EXCLUDED.window_end, alert_count = EXCLUDED.alert_count, computed_at = EXCLUDED.computed_at
"""

spark = (
    SparkSession.builder.master("local[*]")
    .appName("stormstream")
    .config("spark.jars.packages", KAFKA_PACKAGE)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "weather-alerts")
    .option("startingOffsets", "earliest")
    .load()
)


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "15432")),
        dbname=os.getenv("POSTGRES_DB", "stormstream"),
        user=os.getenv("POSTGRES_USER", "stormstream"),
        password=os.getenv("POSTGRES_PASSWORD", "stormstream"),
    )


def iso_col(c):
    # timestamp -> text with an explicit +00:00. pyspark would otherwise hand python
    # a naive datetime in my laptop's timezone and shift everything by 4 hours
    return F.date_format(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'+00:00'")


def iso(name):
    return iso_col(F.col(name)).alias(name)


def parse_alerts(df):
    """kafka rows -> flat columns plus is_valid and reason. works on streaming and batch dataframes"""
    text = F.col("value").cast("string")
    parsed = df.select(text.alias("raw"), F.from_json(text, ALERT_SCHEMA).alias("a"), "partition", "offset")

    flat = parsed.select(
        "raw", "partition", "offset",
        F.col("a.alert_id").alias("alert_id"),
        F.col("a.event_type").alias("event_type"),
        F.col("a.severity").alias("severity"),
        F.col("a.region").alias("region"),
        F.col("a.effective_at").cast("timestamp").alias("effective_at"),
        F.col("a.expires_at").cast("timestamp").alias("expires_at"),
        F.col("a.status").alias("status"),
        F.col("a.ingested_at").cast("timestamp").alias("ingested_at"),
        F.when(F.coalesce(*[F.col(f"a.{n}") for n in FIELDS]).isNull(), "not valid JSON or no known fields")
        .otherwise("missing or invalid field").alias("reason"),
    )

    # valid = every required field present, and severity/status are allowed values
    is_valid = F.lit(True)
    for col in REQUIRED:
        is_valid = is_valid & F.col(col).isNotNull()
    is_valid = F.coalesce(is_valid & F.col("severity").isin(SEVERITIES) & F.col("status").isin(STATUSES), F.lit(False))
    return flat.withColumn("is_valid", is_valid)


# ---------- query 1: raw_alerts + dead letters ----------

def process_batch(batch_df, batch_id):
    flat = parse_alerts(batch_df)

    good_rows = (
        flat.filter(F.col("is_valid"))
        .select("offset", "alert_id", "event_type", "severity", "region",
                iso("effective_at"), iso("expires_at"), "status", iso("ingested_at"))
        .orderBy("offset")
        .collect()
    )
    bad_rows = flat.filter(~F.col("is_valid")).select("raw", "reason", "partition", "offset").collect()

    # if one alert shows up twice in a batch, keep only the latest. postgres won't let
    # a single upsert touch the same row twice. same alert = same partition, so offset order is real order
    latest = {}
    for r in good_rows:
        latest[r.alert_id] = r
    upserts = [(r.alert_id, r.event_type, r.severity, r.region, r.effective_at,
                r.expires_at, r.status, r.ingested_at) for r in latest.values()]
    dead = [(r.raw, r.reason, r.partition, r.offset) for r in bad_rows]

    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:  # commits if it works, rolls back if it fails
            if upserts:
                execute_values(cur, UPSERT_SQL, upserts)
            if dead:
                execute_values(cur, DEAD_LETTER_SQL, dead)
    finally:
        conn.close()

    print(f"--- raw_alerts batch {batch_id}: wrote {len(upserts)} alerts, {len(dead)} dead letters")


# ---------- queries 2 and 3: window counts ----------

# only real, usable alerts count. expired events are left out on purpose, so
# alert_count means "alerts that showed up or changed in this window"
valid_alerts = (parse_alerts(raw)
    .filter(F.col("is_valid") & F.col("status").isin("active", "updated"))
    .withWatermark("ingested_at", WATERMARK)
    .dropDuplicatesWithinWatermark(["alert_id", "status", "severity", "expires_at"]))


def make_window_writer(window_type):
    def write(batch_df, batch_id):
        # update mode: only windows whose count changed show up here, with the total so far
        now = datetime.now(timezone.utc).isoformat()
        rows = [(window_type, r.window_start, r.window_end, r.region, r.severity, r.alert_count, now)
                for r in batch_df.collect()]
        if not rows:
            return
        conn = pg_connect()
        try:
            with conn, conn.cursor() as cur:
                execute_values(cur, WINDOW_UPSERT_SQL, rows)
        finally:
            conn.close()
        print(f"--- {window_type} batch {batch_id}: upserted {len(rows)} window rows")
    return write


def start_window_query(window_type, checkpoint, *window_args):
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


raw.writeStream.queryName("raw_alerts") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "checkpoints/raw_alerts") \
    .trigger(processingTime=TRIGGER) \
    .start()
start_window_query("tumbling_5m", "checkpoints/tumbling_5m", "5 minutes")
start_window_query("sliding_15m", "checkpoints/sliding_15m", "15 minutes", "5 minutes")

spark.streams.awaitAnyTermination()  # runs until ctrl+c
