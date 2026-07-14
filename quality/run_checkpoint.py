#!/usr/bin/env python3
"""
Automated data-quality gate for the SILVER Delta table using Great Expectations.

Reads the most recent slice of silver/market_ticks via Spark, runs a suite of
expectations, prints a readable report, and exits non-zero if any critical
expectation fails — so Airflow (or CI) can block downstream dbt models on bad
data.

Run:
  docker compose exec spark-master python3 /opt/quality/run_checkpoint.py
"""
import os
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from great_expectations.dataset import SparkDFDataset

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/market_ticks")
LOOKBACK_MIN = int(os.getenv("QUALITY_LOOKBACK_MIN", "60"))

KNOWN_SYMBOLS = [
    "RELIANCE", "TCS", "HDFCBANK", "INFY", "ICICIBANK", "HINDUNILVR", "SBIN",
    "BHARTIARTL", "ITC", "KOTAKBANK", "LT", "AXISBANK", "ASIANPAINT", "MARUTI",
    "SUNPHARMA", "TATAMOTORS", "WIPRO", "NESTLEIND", "ULTRACEMCO", "TITAN",
    "SENSEX", "NIFTY50",
]


def build_spark():
    return (
        SparkSession.builder.appName("quality-checks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=LOOKBACK_MIN)
    df = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter(F.col("event_time") >= F.lit(cutoff))
    )
    n = df.count()
    print(f"[quality] validating {n:,} silver rows from the last {LOOKBACK_MIN} min")
    if n == 0:
        print("[quality] WARNING: no rows in window — is the pipeline running?")
        sys.exit(2)

    ge = SparkDFDataset(df)
    results = []

    def check(label, res):
        ok = res["success"]
        results.append((label, ok, res.get("result", {})))
        print(f"  [{'PASS' if ok else 'FAIL'}] {label}")
        return ok

    # --- Completeness / not-null ------------------------------------------
    check("symbol not null", ge.expect_column_values_to_not_be_null("symbol"))
    check("ltp not null", ge.expect_column_values_to_not_be_null("ltp"))
    check("event_time not null", ge.expect_column_values_to_not_be_null("event_time"))

    # --- Validity ---------------------------------------------------------
    check("ltp positive", ge.expect_column_values_to_be_between("ltp", min_value=0, strict_min=True))
    check("bid positive", ge.expect_column_values_to_be_between("bid", min_value=0, strict_min=True))
    check("volume >= 1", ge.expect_column_values_to_be_between("volume", min_value=1))
    check("spread non-negative", ge.expect_column_values_to_be_between("spread", min_value=0))
    check("side in {BUY,SELL}", ge.expect_column_values_to_be_in_set("side", ["BUY", "SELL"]))
    check("symbol in known universe", ge.expect_column_values_to_be_in_set("symbol", KNOWN_SYMBOLS))
    check("exchange in {NSE,BSE}", ge.expect_column_values_to_be_in_set("exchange", ["NSE", "BSE"]))

    # --- Consistency ------------------------------------------------------
    # ask should be >= bid; verify via a derived column expectation
    ge2 = SparkDFDataset(df.withColumn("ask_ge_bid", (F.col("ask") >= F.col("bid")).cast("int")))
    check("ask >= bid", ge2.expect_column_values_to_be_in_set("ask_ge_bid", [1]))

    # --- Freshness (schema presence) --------------------------------------
    for col in ["symbol", "ltp", "bid", "ask", "volume", "event_time", "spread"]:
        check(f"column present: {col}", ge.expect_column_to_exist(col))

    failed = [label for (label, ok, _) in results if not ok]
    print("\n[quality] summary: "
          f"{len(results) - len(failed)}/{len(results)} passed")
    if failed:
        print("[quality] FAILED expectations: " + ", ".join(failed))
        sys.exit(1)
    print("[quality] all expectations passed ✅")


if __name__ == "__main__":
    run()
