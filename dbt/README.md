# dbt transformations (dbt-trino)

Turns the registered Delta tables into analytics-ready gold marts via Trino.

Lineage:

```
delta.market.silver_market_ticks ─┐
                                  ├─ stg_market_ticks ─┐
delta.market.gold_ohlc_1min ──────┴─ stg_ohlc_1min ────┼─ mart_symbol_daily ─┬─ mart_sector_performance
                                                        └─ mart_symbol_volatility
```

Run:

```bash
make dbt-run     # build staging views + marts
make dbt-test    # run schema + dbt_utils tests
```

Prereqs: the streaming job has created the Delta tables and `make register`
has registered them in Trino. First run also needs `dbt deps` for dbt_utils
(the Airflow DAG does this automatically).
