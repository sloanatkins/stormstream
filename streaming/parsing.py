"""
Alert Parsing
=============

Turns raw Kafka rows into clean columns and decides which messages
are valid. Bad messages are flagged with a reason so the streaming job
can send them to the dead letter table.

Kept apart from stream_alerts.py so the tests can import it without
starting the streaming job.

Dependencies:
- PySpark
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# ── Allowed values ───────────────────────────────────────────────────
SEVERITIES = ["Extreme", "Severe", "Moderate", "Minor", "Unknown"]
STATUSES = ["active", "updated", "expired"]

# Fields every valid message must have (expires_at is allowed to be empty)
REQUIRED = ["alert_id", "event_type", "severity", "region", "effective_at", "status", "ingested_at"]

# ── Message schema ───────────────────────────────────────────────────
# Every field a message can have. Everything is read as a string first
# and the timestamps are converted afterwards, so a bad timestamp
# becomes null instead of crashing the job
FIELDS = ["alert_id", "event_type", "severity", "region", "effective_at", "expires_at", "status", "ingested_at"]
ALERT_SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])


def parse_alerts(df: DataFrame) -> DataFrame:
    """
    Parse raw Kafka rows into flat columns and flag the valid ones.

    Works on streaming and batch dataframes.

    Returns:
    --------
    DataFrame with:
    - raw, partition, offset (straight from Kafka)
    - alert_id, event_type, severity, region, effective_at,
      expires_at, status, ingested_at
    - reason (why a row is bad, only meaningful when is_valid is False)
    - is_valid (bool)
    """

    # Decode the message bytes and parse the JSON
    text = F.col("value").cast("string")
    parsed = df.select(text.alias("raw"), F.from_json(text, ALERT_SCHEMA).alias("a"), "partition", "offset")

    # try_cast gives null on a bad timestamp. a plain cast crashes the
    # whole job in Spark 4 and it loops on the same bad message forever
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
        # All fields null means the JSON was broken or had nothing we know
        F.when(F.coalesce(*[F.col(f"a.{n}") for n in FIELDS]).isNull(), "not valid JSON or no known fields")
        .otherwise("missing or invalid field").alias("reason"),
    )

    # Valid = every required field present and severity/status are allowed values
    is_valid = F.lit(True)
    for col in REQUIRED:
        is_valid = is_valid & F.col(col).isNotNull()
    is_valid = F.coalesce(is_valid & F.col("severity").isin(SEVERITIES) & F.col("status").isin(STATUSES), F.lit(False))

    return flat.withColumn("is_valid", is_valid)
