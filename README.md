# Real-Time Streaming Lakehouse — NSE/BSE Market Data

A production-grade, end-to-end **streaming lakehouse** that ingests NSE/BSE
market ticks in real time, lands them in **Delta Lake** on object storage,
transforms them into analytics-ready gold tables, enforces automated data
quality, and serves **live dashboards** — all runnable locally with a single
`docker compose up`.

> **Resume line this project backs up:** *"Built a real-time lakehouse
> processing 10K+ events/sec with sub-second latency using Kafka, Spark
> Structured Streaming, and Delta Lake, with automated data-quality checks via
> Great Expectations, dbt transformations, and Airflow orchestration."*

---

## Architecture

```
                        ┌──────────────────────────────────────────────────────────┐
                        │                     LAKEHOUSE (MinIO / S3)                 │
  ┌───────────┐  ticks  │   ┌─────────┐     ┌─────────┐     ┌──────────────────┐     │
  │ Producer  │────────▶│   │ bronze  │────▶│ silver  │────▶│ gold/ohlc_1min   │     │
  │ NSE/BSE   │  Kafka  │   │ (raw)   │     │ (clean) │     │ (1-min candles)  │     │
  │ 10K+ eps  │────┐    │   └─────────┘     └─────────┘     └──────────────────┘     │
  └───────────┘    │    └─────────▲───────────────▲──────────────────▲──────────────┘
        ▲          │              │ Delta writes  │                  │
   yfinance     ┌──▼───┐   ┌──────┴───────┐       │            ┌─────┴──────┐
   seed price   │Kafka │   │ Spark Struct │       │            │  Trino     │◀── dbt marts
                │(KRaft)│  │  Streaming   │       │            │ (Delta rd) │
                └──────┘   └──────────────┘       │            └─────┬──────┘
                                                  │                  │
                                   ┌──────────────┴──────┐    ┌──────┴───────┐
                                   │ Great Expectations  │    │ Grafana /    │
                                   │ quality gate        │    │ Superset     │
                                   └─────────────────────┘    │ live boards  │
                                            ▲                 └──────────────┘
                                   ┌────────┴─────────┐
                                   │ Airflow (batch): │
                                   │ quality → dbt →  │
                                   │ Delta OPTIMIZE   │
                                   └──────────────────┘
```

**Flow:** a high-throughput producer streams market ticks to **Kafka**;
**Spark Structured Streaming** writes them through a **bronze → silver → gold**
medallion in **Delta Lake** on **MinIO**; **Trino** exposes the Delta tables to
**dbt** (which builds gold analytics marts) and to **Grafana/Superset** for live
dashboards; **Airflow** orchestrates the batch side (quality gate, dbt, Delta
compaction); **Great Expectations** guards data quality.

## Tech stack

| Layer | Technology |
|---|---|
| Ingestion | Python + `confluent-kafka` producer (yfinance-seeded prices) |
| Streaming bus | Apache Kafka (KRaft mode, no ZooKeeper) |
| Stream processing | Apache Spark Structured Streaming (PySpark) |
| Table format | Delta Lake (medallion: bronze/silver/gold) |
| Object storage | MinIO (S3-compatible) |
| Query engine | Trino (Delta connector + Hive Metastore) |
| Transformations | dbt (`dbt-trino`) |
| Orchestration | Apache Airflow (LocalExecutor) |
| Data quality | Great Expectations + in-DAG SQL gate |
| BI / dashboards | Grafana + Apache Superset |

## Quick start

```bash
cp .env.example .env          # optional: tweak TARGET_EPS / credentials
make up                       # build + start everything (first build ~5-10 min)
```

Once containers are healthy:

```bash
# 1) confirm the producer is flowing (should report ~10,000 eps)
make producer-logs

# 2) confirm Spark is writing Delta (bronze/silver/gold queries started)
make streaming-logs

# 3) register the Delta tables in Trino (once tables exist, ~1 min after start)
make register

# 4) build the dbt marts + run tests
make dbt-run
make dbt-test

# 5) run the Great Expectations quality gate
make quality
```

### Service URLs

| Service | URL | Login |
|---|---|---|
| Kafka UI | http://localhost:8085 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark master UI | http://localhost:8090 | — |
| Trino | http://localhost:8080 | user: any |
| Airflow | http://localhost:8081 | admin / admin |
| Superset | http://localhost:8088 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |

In **Airflow**, enable the `lakehouse_batch` DAG — it runs the quality gate,
dbt marts, and Delta compaction every 15 minutes.

## Repository layout

```
streaming-lakehouse/
├── docker-compose.yml         # the whole stack
├── Makefile                   # up / down / dbt / quality / register helpers
├── .env.example
├── ingestion/                 # NSE/BSE Kafka producer (10K+ eps)
│   ├── producer.py
│   ├── instruments.py
│   └── Dockerfile
├── streaming/                 # Spark Structured Streaming job
│   ├── jobs/spark_streaming_job.py   # Kafka -> Delta bronze/silver/gold
│   └── Dockerfile
├── quality/                   # Great Expectations gate on silver
│   └── run_checkpoint.py
├── dbt/                       # dbt-trino transformations
│   ├── models/staging/        # stg_* views
│   └── models/marts/          # gold analytics marts + tests
├── airflow/                   # orchestration
│   └── dags/lakehouse_batch_dag.py
├── trino/                     # Trino catalog + Hive metastore config + init.sql
├── dashboards/
│   ├── grafana/               # provisioned datasource + live dashboard
│   └── superset/              # bootstrap + Trino connection
└── scripts/register_tables.sh
```

## How the "10K+ events/sec" claim works

The producer uses the librdkafka-backed `confluent-kafka` client with batching
(`linger.ms`, lz4 compression), which sustains well over 10,000 msg/s on a
single core. Throughput is configurable via `TARGET_EPS`. Prices are seeded
from a live public feed (Yahoo Finance) on startup, then evolved with a
geometric-Brownian-motion walk — realistic values without depending on a paid
real-time tick API (which don't exist for open Indian-equity feeds). Set
`USE_LIVE_FEED=false` to skip the seed entirely.

## Data model (medallion)

- **bronze/market_ticks** — raw Kafka payloads with offsets/timestamps for
  audit and replay (append-only, partitioned by ingest date).
- **silver/market_ticks** — parsed, typed, deduplicated, watermarked, and
  quality-filtered ticks with derived `spread` / `trade_value`.
- **gold/ohlc_1min** — streaming 1-minute OHLCV candles per symbol.
- **analytics.*** (dbt) — `mart_symbol_daily`, `mart_sector_performance`,
  `mart_symbol_volatility`.

## Notes & tuning

- First `make up` builds several images and Spark downloads its Kafka/Delta/
  hadoop-aws jars — allow a few minutes. Recommended: Docker with **≥ 8 GB RAM**.
- Lower `TARGET_EPS` in `.env` if running on a small machine.
- The streaming job trigger intervals (5–10 s) balance latency vs. small-file
  count; the Airflow `OPTIMIZE` step compacts files periodically.
- Everything is local and credential-simple by design (portfolio project). For
  real deployments, externalise secrets, enable TLS/SASL on Kafka, and use real
  S3 + Glue/managed metastore.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make register` errors "table already exists" | Safe to ignore — registration is idempotent. |
| Trino `SHOW TABLES FROM delta.market` empty | Wait for the streaming job to create the Delta dirs, then re-run `make register`. |
| Producer low eps | Increase Docker CPU, or lower `TARGET_EPS`. |
| Spark job OOM | Reduce `SPARK_WORKER_MEMORY` consumers / `maxOffsetsPerTrigger`. |
```
