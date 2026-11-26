# PII Hygiene Capstone — developer entry points.
SHELL := /bin/bash
PY := .venv/bin/python
DBT := .venv/bin/dbt
BATCH ?= local_dev_001

.PHONY: help up down logs build init data pipeline quick quarantine-demo flood-demo dbt test lint fmt app reset psql evidence

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the full local stack (warehouse, Airflow, OTel, Streamlit)
	docker compose up -d --build

down: ## Stop the stack (keeps data volumes)
	docker compose down

logs: ## Tail Airflow scheduler and webserver logs
	docker compose logs -f airflow-scheduler airflow-webserver

data: ## Download the real Hugging Face corpora (resumable, ~5GB)
	$(PY) -m ingestion.download_sources --include-validation

pipeline: ## Run the full pipeline on the host against the Docker warehouse
	scripts/run_local_pipeline.sh --batch-id $(BATCH)

quick: ## Fast smoke pipeline (2k docs/source, 60 PDFs)
	scripts/run_local_pipeline.sh --quick --batch-id smoke_001

quarantine-demo: ## Inject 3 corrupt records and rebuild dbt (marts must stay clean)
	$(PY) -m ingestion.inject_bad_records --batch-id test_quarantine_01 --mode sample
	$(DBT) run --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene
	$(DBT) test --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene

flood-demo: ## Inject >5% bad rows to trip the circuit breaker (agent must be blocked)
	$(PY) -m ingestion.inject_bad_records --batch-id test_flood_01 --mode flood
	$(DBT) run --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene
	-$(DBT) test --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene

dbt: ## Run dbt build and tests
	$(DBT) run --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene
	$(DBT) test --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene

test: ## Unit tests (no database required)
	$(PY) -m pytest tests -q

lint: ## Ruff lint
	$(PY) -m ruff check .

fmt: ## Ruff format
	$(PY) -m ruff format .

app: ## Run Streamlit on the host against the Docker warehouse
	PII_DB_HOST=127.0.0.1 PII_DB_PORT=5433 $(PY) -m streamlit run app/streamlit_app.py

reset: ## Destroy containers AND warehouse/data volumes (destructive)
	docker compose down -v
	rm -rf data/staged data/unstructured data/observability

psql: ## Open psql in the local warehouse
	docker compose exec warehouse psql -U pii_admin -d pii_lab

evidence: ## Regenerate the verification report in docs/evidence.md
	$(PY) -m scripts.collect_evidence
