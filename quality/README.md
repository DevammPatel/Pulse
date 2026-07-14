# Data Quality (Great Expectations)

`run_checkpoint.py` validates the **silver** Delta table on every batch run and
exits non-zero when a critical expectation fails, so Airflow gates downstream
dbt models on clean data.

Expectations enforced:

- **Completeness** — `symbol`, `ltp`, `event_time` are never null.
- **Validity** — `ltp`/`bid` strictly positive, `volume >= 1`, `spread >= 0`,
  `side ∈ {BUY, SELL}`, `exchange ∈ {NSE, BSE}`, `symbol` within the known
  instrument universe.
- **Consistency** — `ask >= bid` on every row.
- **Schema** — required columns exist.

Run manually:

```bash
make quality
# or
docker compose exec spark-master python3 /opt/quality/run_checkpoint.py
```

Tune the validation window with `QUALITY_LOOKBACK_MIN` (default 60 minutes).
