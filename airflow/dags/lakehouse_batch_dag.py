"""
Batch orchestration for the streaming lakehouse.

Runs every 15 minutes and:
  1. Registers any newly-created Delta tables in Trino (idempotent).
  2. Runs a data-quality gate against the silver table (blocks the rest on fail).
  3. Builds the dbt marts (staging views + gold analytics tables).
  4. Runs dbt tests.
  5. Compacts the Delta tables (OPTIMIZE) to keep small streaming files in check.

The Spark Structured Streaming job runs continuously outside Airflow; this DAG
handles the *batch* side of the lakehouse (transformations, quality, upkeep).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
DBT_DIR = "/opt/airflow/dbt"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _trino_conn():
    import trino
    return trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow",
                               catalog="delta")


def register_delta_tables():
    """Register the Spark-written Delta tables in Trino (ignore if present)."""
    stmts = [
        "CREATE SCHEMA IF NOT EXISTS delta.market WITH (location = 's3://lakehouse/')",
        ("CALL delta.system.register_table(schema_name => 'market', "
         "table_name => 'bronze_market_ticks', "
         "table_location => 's3://lakehouse/bronze/market_ticks')"),
        ("CALL delta.system.register_table(schema_name => 'market', "
         "table_name => 'silver_market_ticks', "
         "table_location => 's3://lakehouse/silver/market_ticks')"),
        ("CALL delta.system.register_table(schema_name => 'market', "
         "table_name => 'gold_ohlc_1min', "
         "table_location => 's3://lakehouse/gold/ohlc_1min')"),
    ]
    conn = _trino_conn()
    cur = conn.cursor()
    for s in stmts:
        try:
            cur.execute(s)
            cur.fetchall()
        except Exception as e:  # already registered / exists -> fine
            print(f"register skip: {e}")


def data_quality_gate():
    """Assert critical invariants on the silver table; raise to block the DAG."""
    conn = _trino_conn()
    cur = conn.cursor()
    checks = {
        "rows_present":
            ("select count(*) from delta.market.silver_market_ticks "
             "where event_time > current_timestamp - interval '60' minute", lambda v: v > 0),
        "no_null_symbol":
            ("select count(*) from delta.market.silver_market_ticks where symbol is null",
             lambda v: v == 0),
        "no_nonpositive_ltp":
            ("select count(*) from delta.market.silver_market_ticks where ltp <= 0",
             lambda v: v == 0),
        "ask_ge_bid":
            ("select count(*) from delta.market.silver_market_ticks where ask < bid",
             lambda v: v == 0),
        "known_exchanges":
            ("select count(*) from delta.market.silver_market_ticks "
             "where exchange not in ('NSE','BSE')", lambda v: v == 0),
    }
    failures = []
    for name, (sql, ok) in checks.items():
        cur.execute(sql)
        val = cur.fetchone()[0]
        status = "PASS" if ok(val) else "FAIL"
        print(f"[quality] {name}: {val} -> {status}")
        if not ok(val):
            failures.append(name)
    if failures:
        raise ValueError(f"Data quality gate failed: {failures}")


def optimize_delta():
    """Compact small streaming files across the medallion tables."""
    conn = _trino_conn()
    cur = conn.cursor()
    for tbl in ["bronze_market_ticks", "silver_market_ticks", "gold_ohlc_1min"]:
        try:
            cur.execute(f"ALTER TABLE delta.market.{tbl} EXECUTE optimize")
            cur.fetchall()
            print(f"optimized {tbl}")
        except Exception as e:
            print(f"optimize skip {tbl}: {e}")


with DAG(
    dag_id="lakehouse_batch",
    description="Quality gate + dbt marts + Delta compaction over streaming data",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dbt", "quality", "delta"],
) as dag:

    register = PythonOperator(
        task_id="register_delta_tables",
        python_callable=register_delta_tables,
    )

    quality = PythonOperator(
        task_id="data_quality_gate",
        python_callable=data_quality_gate,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir .",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir .",
    )

    optimize = PythonOperator(
        task_id="optimize_delta",
        python_callable=optimize_delta,
    )

    register >> quality >> dbt_deps >> dbt_run >> dbt_test >> optimize
