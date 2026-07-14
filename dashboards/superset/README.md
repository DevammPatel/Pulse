# Superset

Superset boots with a pre-registered **Trino Lakehouse** database connection
(`trino://admin@trino:8080/delta`).

Login at http://localhost:8088 (admin / admin), then:

1. **SQL Lab** → pick the *Trino Lakehouse* database → query the gold marts:
   ```sql
   select * from delta.analytics.mart_sector_performance;
   select * from delta.market.gold_ohlc_1min order by window_start desc limit 100;
   ```
2. Save a query as a **dataset**, then build charts/dashboards on top (e.g. a
   candlestick from `gold_ohlc_1min`, a sector-turnover bar from
   `mart_sector_performance`).

Superset auto-refresh (dashboard settings → set a refresh interval, e.g. 10s)
gives you the live dashboard on top of the streaming data.
