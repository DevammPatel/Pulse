#!/usr/bin/env python3
"""
Spark Structured Streaming: Kafka market.ticks -> Delta Lake (bronze/silver/gold)

Medallion layout on MinIO (s3a://lakehouse):
  bronze/market_ticks   raw payloads, append-only, full audit trail
  silver/market_ticks   parsed + typed + deduped + watermarked, quality-clean
  gold/ohlc_1min        1-minute OHLCV candles per symbol (streaming aggregate)

Each layer is an independent streaming query with its own checkpoint, so they
can fail and recover independently. Sub-second micro-batches give near
real-time latency; the trigger interval is tunable below.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.ticks")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")

LAKE = "s3a://lakehouse"
CKPT = f"{LAKE}/_checkpoints"

TICK_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("exchange", StringType()),
    StructField("sector", StringType()),
    StructField("ltp", DoubleType()),
    StructField("bid", DoubleType()),
    StructField("ask", DoubleType()),
    StructField("volume", LongType()),
    StructField("side", StringType()),
    StructField("event_time", StringType()),
])


def build_spark():
    return (
        SparkSession.builder.appName("market-lakehouse-streaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # S3A -> MinIO
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.streaming.schemaInference", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # -------------------------------------------------------------------------
    # Source: Kafka
    # -------------------------------------------------------------------------
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 200000)  # backpressure guard
        .load()
    )

    # -------------------------------------------------------------------------
    # BRONZE — raw, append-only. Keep Kafka metadata for lineage/replay.
    # -------------------------------------------------------------------------
    bronze = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic"), F.col("partition"), F.col("offset"),
        F.col("timestamp").alias("kafka_ingest_time"),
        F.date_format(F.current_timestamp(), "yyyy-MM-dd").alias("ingest_date"),
    )

    bronze_q = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/bronze_market_ticks")
        .partitionBy("ingest_date")
        .trigger(processingTime="5 seconds")
        .start(f"{LAKE}/bronze/market_ticks")
    )

    # -------------------------------------------------------------------------
    # SILVER — parsed, typed, quality-clean, deduped, watermarked.
    # -------------------------------------------------------------------------
    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), TICK_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        # basic quality filters (bad rows are dropped from silver, kept in bronze)
        .filter(F.col("symbol").isNotNull())
        .filter(F.col("ltp") > 0)
        .filter(F.col("bid") > 0)
        .filter(F.col("ask") >= F.col("bid"))
        .withColumn("spread", F.round(F.col("ask") - F.col("bid"), 4))
        .withColumn("trade_value", F.round(F.col("ltp") * F.col("volume"), 2))
        .withColumn("event_date", F.to_date("event_time"))
        .withWatermark("event_time", "30 seconds")
        .dropDuplicates(["symbol", "event_time", "ltp", "volume"])
    )

    silver_q = (
        parsed.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/silver_market_ticks")
        .partitionBy("event_date")
        .trigger(processingTime="5 seconds")
        .start(f"{LAKE}/silver/market_ticks")
    )

    # -------------------------------------------------------------------------
    # GOLD — 1-minute OHLCV candles per symbol (streaming windowed aggregate).
    # -------------------------------------------------------------------------
    ohlc = (
        parsed.withWatermark("event_time", "1 minute")
        .groupBy(
            F.window("event_time", "1 minute").alias("w"),
            F.col("symbol"), F.col("exchange"), F.col("sector"),
        )
        .agg(
            F.first("ltp", ignorenulls=True).alias("open"),
            F.max("ltp").alias("high"),
            F.min("ltp").alias("low"),
            F.last("ltp", ignorenulls=True).alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("trade_value").alias("turnover"),
            F.count("*").alias("tick_count"),
            F.avg("spread").alias("avg_spread"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "symbol", "exchange", "sector",
            "open", "high", "low", "close",
            "volume", "turnover", "tick_count", "avg_spread",
            F.to_date(F.col("w.start")).alias("event_date"),
        )
    )

    gold_q = (
        ohlc.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CKPT}/gold_ohlc_1min")
        .partitionBy("event_date")
        .trigger(processingTime="10 seconds")
        .start(f"{LAKE}/gold/ohlc_1min")
    )

    print("[streaming] bronze/silver/gold queries started", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
