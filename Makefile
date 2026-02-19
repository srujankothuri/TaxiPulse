# ============================================================
# TaxiPulse — Makefile
# Convenience commands for running the project
# ============================================================

.PHONY: help up down restart ps logs test dashboard ingestion quality silver gold anomaly streaming clean

# Default
help: ## Show this help message
	@echo "🚕 TaxiPulse — Available Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ---- Infrastructure ----

up: ## Start all Docker services
	docker-compose up -d
	@echo "✅ All services started"
	@echo "   Airflow:  http://localhost:8080 (admin/admin)"
	@echo "   MinIO:    http://localhost:9001 (taxipulse/taxipulse123)"
	@echo "   Postgres: localhost:5432"

down: ## Stop all Docker services
	docker-compose down
	@echo "✅ All services stopped"

restart: ## Restart all Docker services
	docker-compose down
	docker-compose up -d

ps: ## Show running containers
	docker-compose ps

logs: ## Show logs (use: make logs s=kafka)
	docker-compose logs $(s)

# ---- Pipeline Steps ----

ingestion: ## Run batch ingestion (download + upload to MinIO + load Bronze)
	python scripts/run_ingestion.py

quality: ## Run data quality validation
	python scripts/run_quality_check.py

silver: ## Run Silver layer transformation
	python scripts/run_silver.py

gold: ## Run Gold layer build (star schema + aggregations)
	python scripts/run_gold.py

anomaly: ## Run anomaly detection + alerting
	python scripts/run_anomaly_detection.py

streaming: ## Run Kafka streaming demo (500 events)
	python scripts/run_streaming_demo.py

zones: ## Load NYC zone names into dimension tables
	python scripts/load_zone_names.py

# ---- Full Pipeline ----

pipeline: ingestion quality silver gold anomaly ## Run the complete batch pipeline
	@echo ""
	@echo "✅ Full pipeline complete!"

# ---- Dashboard ----

dashboard: ## Launch Streamlit dashboard
	python -m streamlit run streamlit_app/app.py

# ---- Testing ----

test: ## Run all pytest tests
	python -m pytest tests/ -v

# ---- Data Export (for Streamlit Cloud) ----

export-data: ## Export Gold tables to CSV for Streamlit Cloud deployment
	mkdir -p streamlit_app/data
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY gold.agg_daily_summary TO STDOUT WITH CSV HEADER" > streamlit_app/data/agg_daily_summary.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY gold.agg_hourly_zone_revenue TO STDOUT WITH CSV HEADER" > streamlit_app/data/agg_hourly_zone_revenue.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY gold.anomaly_log TO STDOUT WITH CSV HEADER" > streamlit_app/data/anomaly_log.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY gold.quality_log TO STDOUT WITH CSV HEADER" > streamlit_app/data/quality_log.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY gold.dim_pickup_location TO STDOUT WITH CSV HEADER" > streamlit_app/data/dim_pickup_location.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT COUNT(*) as cnt FROM bronze.raw_yellow_trips) TO STDOUT WITH CSV HEADER" > streamlit_app/data/bronze_count.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT COUNT(*) as cnt FROM silver.clean_yellow_trips) TO STDOUT WITH CSV HEADER" > streamlit_app/data/silver_count.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT COUNT(*) as cnt FROM silver.quarantined_yellow_trips) TO STDOUT WITH CSV HEADER" > streamlit_app/data/quarantine_count.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT COUNT(*) as cnt FROM gold.fact_trips) TO STDOUT WITH CSV HEADER" > streamlit_app/data/fact_count.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT COUNT(*) as cnt FROM gold.dim_datetime) TO STDOUT WITH CSV HEADER" > streamlit_app/data/dim_datetime_count.csv
	docker exec -it taxipulse-postgres psql -U taxipulse -d taxipulse \
		-c "COPY (SELECT MIN(pickup_datetime) as earliest, MAX(pickup_datetime) as latest, COUNT(DISTINCT pickup_datetime::date) as total_days FROM silver.clean_yellow_trips) TO STDOUT WITH CSV HEADER" > streamlit_app/data/freshness.csv
	@echo "✅ Data exported to streamlit_app/data/"

# ---- Cleanup ----

clean: ## Stop services and remove all data volumes (DESTRUCTIVE)
	docker-compose down -v
	@echo "⚠️  All data volumes removed"

clean-python: ## Remove Python cache files
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "✅ Python cache cleaned"