#!/usr/bin/env bash
# Run the full pipeline on the host against the Dockerized warehouse.
# Usage: scripts/run_local_pipeline.sh [--quick] [--skip-generate]
#        [--limit=N] [--doc-count=N] [--batch-id=B]
set -euo pipefail

cd "$(dirname "$0")/.."

# Host defaults: warehouse is published on 127.0.0.1:5433.
export PII_DB_HOST="${PII_DB_HOST:-127.0.0.1}"
export PII_DB_PORT="${PII_DB_PORT:-5433}"
export PII_DB_NAME="${PII_DB_NAME:-pii_lab}"
export PII_DB_USER="${PII_DB_USER:-pii_admin}"
export PII_DB_PASSWORD="${PII_DB_PASSWORD:-pii_admin_pw}"
export PII_DATA_DIR="${PII_DATA_DIR:-$(pwd)/data}"
export PII_BATCH_ID="${PII_BATCH_ID:-local_dev_001}"
export DBT_TARGET="${DBT_TARGET:-postgres}"
PY="${PYTHON:-.venv/bin/python}"
DBT="${DBT:-.venv/bin/dbt}"

LIMIT=0
DOC_COUNT=1000
SKIP_GENERATE=0
for arg in "$@"; do
  case "$arg" in
    --quick) LIMIT=2000; DOC_COUNT=60 ;;
    --skip-generate) SKIP_GENERATE=1 ;;
    --limit=*) LIMIT="${arg#*=}" ;;
    --doc-count=*) DOC_COUNT="${arg#*=}" ;;
    --batch-id=*) export PII_BATCH_ID="${arg#*=}" ;;
    *) echo "unknown argument: $arg"; exit 2 ;;
  esac
done

LIMIT_ARGS=()
if [ "$LIMIT" -gt 0 ]; then
  LIMIT_ARGS=(--limit-per-source "$LIMIT")
fi

echo "== batch: $PII_BATCH_ID (limit=$LIMIT, docs=$DOC_COUNT) =="
[ -f data/raw/openpii_train.jsonl ] || "$PY" -m ingestion.download_sources

"$PY" -m ingestion.build_batches --batch-id "$PII_BATCH_ID" "${LIMIT_ARGS[@]}"

if [ "$SKIP_GENERATE" -eq 0 ]; then
  "$PY" -m ingestion.generate_unstructured_docs --batch-id "$PII_BATCH_ID" --count "$DOC_COUNT"
  "$PY" -m ingestion.ocr_extract_worker --batch-id "$PII_BATCH_ID"
fi

"$PY" -m ingestion.load_raw --batch-id "$PII_BATCH_ID"

"$DBT" run --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene --target "$DBT_TARGET"
"$DBT" test --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene --target "$DBT_TARGET"

RUN_ID="eval_$(date -u +%Y%m%dT%H%M%SZ)"
"$PY" -m ml.evaluate_pii_detectors --eval-run-id "$RUN_ID"
"$PY" -m agent.triage_agent
"$PY" -m scripts.pipeline_summary --batch-id "$PII_BATCH_ID"
