"""Turns raw Kafka rows into clean columns and decides which messages are valid.
Kept apart from stream_alerts.py so tests can import it without starting the streaming job."""
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]
STATUSES = ["active", "updated", "expired"]
REQUIRED = ["alert_id", "event_type", "severity", "region", "effective_at", "status", "ingested_at"]

# what a good message looks like. everything is read as a string first and the
# timestamps are cast below, so a bad timestamp turns into null and gets caught
FIELDS = ["alert_id", "event_type", "severity", "region", "effective_at", "expires_at", "status", "ingested_at"]
ALERT_SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])


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
        F.expr("try_cast(a.effective_at AS TIMESTAMP)").alias("effective_at"),
        F.expr("try_cast(a.expires_at AS TIMESTAMP)").alias("expires_at"),
        F.col("a.status").alias("status"),
        F.expr("try_cast(a.ingested_at AS TIMESTAMP)").alias("ingested_at"),
        F.when(F.coalesce(*[F.col(f"a.{n}") for n in FIELDS]).isNull(), "not valid JSON or no known fields")
        .otherwise("missing or invalid field").alias("reason"),
    )

    # valid = every required field present, and severity/status are allowed values
    is_valid = F.lit(True)
    for col in REQUIRED:
        is_valid = is_valid & F.col(col).isNotNull()
    is_valid = F.coalesce(is_valid & F.col("severity").isin(SEVERITIES) & F.col("status").isin(STATUSES), F.lit(False))
    return flat.withColumn("is_valid", is_valid)
