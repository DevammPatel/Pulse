#!/usr/bin/env bash
# Register the Spark-written Delta tables in Trino's metastore.
# Safe to re-run; register_table is idempotent-ish (ignore "already exists").
set -uo pipefail

echo "Waiting for Trino to be ready..."
until docker compose exec -T trino trino --execute "SELECT 1" >/dev/null 2>&1; do
  sleep 3
done

echo "Registering Delta tables..."
docker compose exec -T trino trino -f /dev/stdin < trino/init.sql || true
echo "Done. Try:  docker compose exec trino trino --execute \"SHOW TABLES FROM delta.market\""
