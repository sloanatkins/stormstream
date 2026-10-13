"""Spark job: Kafka -> Postgres. Built up one step at a time."""
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"

SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]
STATUSES = ["active", "updated", "expired"]
REQUIRED = ["alert_id", "event_type", "severity", "region", "effective_at", "status", "ingested_at"]

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


def iso(col):
    # timestamp -> text with an explicit +00:00. pyspark would otherwise hand python
    # a naive datetime in my laptop's timezone and shift everything by 4 hours
    return F.date_format(F.col(col), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'+00:00'").alias(col)


def process_batch(batch_df, batch_id):
    text = F.col("value").cast("string")
    parsed = batch_df.select(text.alias("raw"), F.from_json(text, ALERT_SCHEMA).alias("a"), "partition", "offset")

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
        F.when(F.coalesce(*[F.col(f"a.{n}") for n in FIELDS]).isNull(), "not valid JSON or no known fields").otherwise("missing or invalid field").alias("reason"),
    )

    # valid = every required field present, and severity/status are allowed values
    is_valid = F.lit(True)
    for col in REQUIRED:
        is_valid = is_valid & F.col(col).isNotNull()
    is_valid = F.coalesce(is_valid & F.col("severity").isin(SEVERITIES) & F.col("status").isin(STATUSES), F.lit(False))

    good_rows = (
        flat.filter(is_valid)
        .select("offset", "alert_id", "event_type", "severity", "region",
                iso("effective_at"), iso("expires_at"), "status", iso("ingested_at"))
        .orderBy("offset")
        .collect()
    )
    bad_rows = flat.filter(~is_valid).select("raw", "reason", "partition", "offset").collect()

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

    print(f"--- batch {batch_id}: wrote {len(upserts)} alerts, {len(dead)} dead letters")


query = (
    raw.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", "checkpoints/raw_alerts")  # bookmark of which offsets are done
    .trigger(processingTime="10 seconds")  # look for new messages every 10s, runs until ctrl+c
    .start()
)
query.awaitTermination()
