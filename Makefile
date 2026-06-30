# Sales Data Warehouse — task entrypoints
#
# Targets are grouped: setup -> infra -> production pipeline ->
# orchestration -> dev tools -> maintenance.
#
# Run `make help` to see the full list with descriptions.

.PHONY: help setup install up down build test observability report \
        dagster dev-simulate dev-reset-data clean

DBT_DIR := dbt_project
DAGSTER_DIR := dagster_project
SLACK_WEBHOOK_URL ?=  # falls back to .env via direnv

# ============================================================
# Help (default target)
# ============================================================

help: ## Show this help (default target)
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================
# Onboarding (one-time)
# ============================================================

setup: ## First-time setup: docker + venv + python deps + dbt deps
	docker compose up -d postgres
	python3 -m venv venv
	./venv/bin/pip install --upgrade pip
	./venv/bin/pip install -r requirements.txt
	./venv/bin/pip install 'dagster' 'dagster-dbt' 'dagster-webserver' \
	                       'elementary-data[postgres]' 'dbt-metricflow[postgres]'
	cd $(DBT_DIR) && ../venv/bin/dbt deps --profiles-dir .
	@echo ""
	@echo "Setup complete. Next steps:"
	@echo "  1. cp .env.example .env  (then edit credentials + SLACK_WEBHOOK_URL)"
	@echo "  2. direnv allow ."
	@echo "  3. make build"

install: ## Reinstall Python dependencies only
	./venv/bin/pip install -r requirements.txt

# ============================================================
# Infrastructure
# ============================================================

up: ## Start Postgres container
	docker compose up -d postgres

down: ## Stop all containers
	docker compose down

# ============================================================
# Production pipeline
# ============================================================

build: ## Run the full dbt pipeline (seeds + models + snapshot + tests)
	cd $(DBT_DIR) && dbt build --profiles-dir .

test: ## Run dbt tests only
	cd $(DBT_DIR) && dbt test --profiles-dir .

observability: ## Push any pending Elementary alerts (anomalies, freshness) to Slack
	cd $(DBT_DIR) && edr monitor --profiles-dir . --slack-webhook "$(SLACK_WEBHOOK_URL)"

report: ## Generate Elementary HTML observability dashboard
	cd $(DBT_DIR) && edr report --profiles-dir .
	@echo "Report at: $(DBT_DIR)/edr_target/elementary_report.html"

# ============================================================
# Orchestration
# ============================================================

dagster: ## Start Dagster UI on http://localhost:3000
	cd $(DAGSTER_DIR) && dagster dev --host 127.0.0.1 --port 3000

# ============================================================
# Dev only (not part of production pipeline)
# ============================================================

dev-simulate: ## [dev] Simulate one business day of new orders + customer mutations
	./venv/bin/python scripts/simulator/simulate_daily_ingestion.py

dev-reset-data: ## [dev] Restore dbt seeds to git baseline
	./scripts/simulator/reset_data.sh

# ============================================================
# Maintenance
# ============================================================

clean: ## Remove dbt local artifacts (target / logs / packages / edr cache)
	rm -rf $(DBT_DIR)/target $(DBT_DIR)/logs $(DBT_DIR)/dbt_packages $(DBT_DIR)/edr_target
	rm -rf edr_target
