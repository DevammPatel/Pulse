-- Register the Delta tables Spark writes to MinIO so Trino / dbt / BI can query
-- them. Run once after the streaming job has created each table directory:
--   make register    (see Makefile / scripts/register_tables.sh)
--
-- Note: Trino's native S3 filesystem uses the s3:// scheme; Spark writes with
-- s3a://. They point at the same MinIO bucket, and Delta stores relative paths
-- in its log, so the schemes interoperate cleanly.

CREATE SCHEMA IF NOT EXISTS delta.market
  WITH (location = 's3://lakehouse/');

CALL delta.system.register_table(
  schema_name => 'market',
  table_name => 'bronze_market_ticks',
  table_location => 's3://lakehouse/bronze/market_ticks');

CALL delta.system.register_table(
  schema_name => 'market',
  table_name => 'silver_market_ticks',
  table_location => 's3://lakehouse/silver/market_ticks');

CALL delta.system.register_table(
  schema_name => 'market',
  table_name => 'gold_ohlc_1min',
  table_location => 's3://lakehouse/gold/ohlc_1min');
