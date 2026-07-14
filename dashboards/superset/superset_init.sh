#!/bin/sh
# Bootstraps Superset: installs the Trino driver, initialises the metadata DB,
# creates an admin user, registers the Trino database connection, then serves.
set -e

pip install --no-cache-dir "trino[sqlalchemy]" sqlalchemy-trino || true

superset db upgrade

superset fab create-admin \
  --username admin --firstname admin --lastname admin \
  --email admin@example.com --password admin || true

superset init

# Register the Trino connection so the Delta marts are queryable in the UI.
superset set-database-uri \
  --database_name "Trino Lakehouse" \
  --uri "trino://admin@trino:8080/delta" || true

/usr/bin/run-server.sh
