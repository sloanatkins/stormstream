"""Throwaway experiment: what do tumbling and sliding windows do? Prints only, writes nothing."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
FIELDS = ["alert_id", "event_type", "severity", "region", "effective_at", "expires_at", "status", "ingested_at"]
SCHEMA = StructType([StructField(name, StringType()) for name in FIELDS])

spark = (
    SparkSession.builder.master("local[*]")
    .appName("window-experiment")
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

alerts = (
    raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("a"))
    .select("a.*")
    .withColumn("ingested_at", F.col("ingested_at").cast("timestamp"))
    .filter(F.col("status").isin("active", "updated") & F.col("region").isNotNull()
            & F.col("severity").isNotNull() & F.col("ingested_at").isNotNull())
    .withWatermark("ingested_at", "10 minutes")  # event time column + how late is too late
)


def start(kind, *window_args):
    counts = alerts.groupBy(F.window("ingested_at", *window_args), "region", "severity").count()
    return (
        counts.select(F.lit(kind).alias("kind"), F.col("window.start").alias("start"),
                      F.col("window.end").alias("end"), "region", "severity",
                      F.col("count").alias("alert_count"))
        .writeStream.outputMode("update").format("console")
        .option("truncate", "false").option("numRows", "50")
        .trigger(availableNow=True).start()
    )


tumbling = start("tumbling_5m", "5 minutes")
sliding = start("sliding_15m", "15 minutes", "5 minutes")  # 15 min long, new one every 5 min
tumbling.awaitTermination()
sliding.awaitTermination()
