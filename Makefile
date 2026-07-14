# ============================================================================
# Convenience targets for the streaming lakehouse
# ============================================================================
.PHONY: help up down logs ps clean topics producer-logs streaming-logs \
        dbt-run dbt-test quality trino-cli seed-superset

help:            ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	 awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up:              ## Start the entire stack
	docker compose up -d --build
	@echo "Stack starting. UIs:"
	@echo "  Kafka UI     http://localhost:8085"
	@echo "  MinIO        http://localhost:9001  (minioadmin/minioadmin)"
	@echo "  Spark master http://localhost:8090"
	@echo "  Trino        http://localhost:8080"
	@echo "  Airflow      http://localhost:8081  (admin/admin)"
	@echo "  Superset     http://localhost:8088  (admin/admin)"
	@echo "  Grafana      http://localhost:3000  (admin/admin)"

down:            ## Stop the stack (keep volumes)
	docker compose down

clean:           ## Stop the stack and wipe all data volumes
	docker compose down -v

ps:              ## Show container status
	docker compose ps

logs:            ## Tail logs for all services
	docker compose logs -f

producer-logs:   ## Tail producer logs (throughput reporting)
	docker compose logs -f producer

streaming-logs:  ## Tail Spark streaming job logs
	docker compose logs -f spark-streaming

topics:          ## List Kafka topics
	docker compose exec kafka kafka-topics --bootstrap-server localhost:29092 --list

register:        ## Register Spark-written Delta tables in Trino
	bash scripts/register_tables.sh

trino-cli:       ## Open a Trino SQL shell
	docker compose exec trino trino

dbt-run:         ## Run dbt models (staging -> marts)
	docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt run --profiles-dir ."

dbt-test:        ## Run dbt data tests
	docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/dbt && dbt test --profiles-dir ."

quality:         ## Run Great Expectations checks against silver tables
	docker compose exec spark-master python3 /opt/quality/run_checkpoint.py
