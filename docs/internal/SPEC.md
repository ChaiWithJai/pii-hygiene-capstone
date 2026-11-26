# INTERNAL BUILD SPEC — PII Hygiene Capstone (local-first)

> This file is the single source of truth for all parallel workstreams.
> Do not change shared contracts (table names, columns, file paths, CLI names)
> without updating this file. Every component must run with `docker compose up`
> and also on the host with `PII_DB_HOST=127.0.0.1 PII_DB_PORT=5433`.

## Product goal

A fully local, open-source implementation of `prd.md` / `rfc.md`:

- 1M+ unique PII documents from 4 corpora (Parquet / CSV / JSONL) at document grain.
- 1,000 synthetic unstructured PDFs/PNGs with OCR + bbox provenance.
- dbt warehouse `RAW -> STAGING -> INTERMEDIATE -> MARTS`, split-routing quarantine, circuit breaker.
- Reproducible ML evaluation (grouped splits, regex vs. local NER, slices, bootstrapped 95% CIs, latency).
- Quality-gated triage agent writing to `governance.agent_review_queue` + immutable `governance.agent_action_log`.
- 4-tab Streamlit reviewer app with side-by-side PDF/page traceability and HITL approve/dismiss.
- OCI containers, OTel trace ids, OpenLineage events, port path to Snowflake/BigQuery documented.

Local warehouse = Postgres 16 (schemas mirror Snowflake). The dbt project must keep
Snowflake/BigQuery target branches in macros so porting is a target swap, not a rewrite.

## Non-negotiable engineering rules

1. Python 3.12. Type hints on public functions. Docstrings. PEP8/ruff clean (`ruff check .`).
2. No secrets in code. All config via env (`common.config.get_settings()`); `.env.example` is the contract.
3. All pipeline steps idempotent for a given `batch_id` (DELETE batch then reload; dbt models rebuild).
4. Never mutate `raw.*` rows in place. Quarantine is append-only/rebuildable.
5. Every stage writes a trace id: wrap the work in `common.telemetry.traced(stage)`.
6. Every stage emits an OpenLineage event with `common.lineage.emit_event(...)` (START + COMPLETE).
7. No network required at runtime. Data download is an explicit, optional step;
   the pipeline must also run from committed synthetic fixtures.
8. Do not commit data files or model binaries. `data/` is gitignored.
9. Tests must pass without Docker: unit tests for pure logic; DB integration tests skip if no DB.
10. Keep the repo clean: no `__pycache__`, no generated parquet/pdf, no `.env`.

## Repository layout (owners)

```
common/                    # DONE (shared config/db/telemetry/lineage) — do not edit
infra/postgres/init/       # DONE (schemas, roles, governance DDL) — do not edit
docker-compose.yml         # orchestrator-owned
docker/*.Dockerfile        # orchestrator-owned
requirements-*.txt         # orchestrator-owned
ingestion/                 # Workstream A
  __init__.py settings.py?  (config comes from common.config; no duplicate settings)
  download_sources.py
  build_batches.py
  generate_unstructured_docs.py
  ocr_extract_worker.py
  load_raw.py
  inject_bad_records.py
  fixtures.py
dbt_pii_hygiene/           # Workstream B
ml/evaluate_pii_detectors.py   # Workstream C
agent/triage_agent.py          # Workstream C
app/streamlit_app.py           # Workstream D
docs/ + README.md + helm/      # Workstream D
airflow/dags/pii_hygiene_pipeline.py  # orchestrator-owned
scripts/run_local_pipeline.sh          # orchestrator-owned
tests/                     # each workstream adds its own test file
```

## Shared contracts

### Environment (see `.env.example` and `common/config.py`)

`PII_BATCH_ID`, `PII_DATA_DIR`, `PII_DB_HOST/PORT/NAME/USER/PASSWORD`,
`PII_QUARANTINE_THRESHOLD`, `PII_EVAL_MAX_DOCS`, `PII_EVAL_BOOTSTRAP`,
`PII_AGENT_MAX_ACTIONS`, `OTEL_EXPORTER_OTLP_ENDPOINT`, `OPENLINEAGE_URL`,
`SANDBOX_GITHUB_REPO`, `SANDBOX_GITHUB_TOKEN`.

### `raw.pii_documents_raw` (structured landing, document grain)

`batch_id, document_key, source_dataset, source_format, record_id, raw_text,
spans_json (jsonb array of {start:int,end:int,value:str,label:str}), language,
domain, template_hash, ingested_at`

- `document_key = sha256(f"{source_dataset}|{record_id}").hexdigest()` (64 hex chars).
- `record_id`: stable id per source row (`uid`, Nemotron `uid`, SPY row index, pii-scope id).
- `template_hash = sha256(masked/templated text).hexdigest()[:32]` — source row's
  masked_text (OpenPII), text_tagged (Nemotron), placeholder-joined tokens (SPY),
  or masked template (PII-SCOPE fixture). Used for group-aware splitting.
- Include `source_dataset` values exactly:
  `ai4privacy/pii-masking-openpii-1.5m`, `nvidia/Nemotron-PII`,
  `Protegrity/pii-scope`, `mks-logic/SPY`.
- `source_format`: `JSONL`, `PARQUET`, `CSV`.
- Span `start`/`end` are zero-based, end-exclusive Python string indices.

### `raw.unstructured_extractions_raw` (field grain, one row per found PII span)

Columns per DDL. `extraction_key = sha256(document_key|page_number|span_index)` (64 hex).
`source_file_uri = "local://unstructured/<batch_id>/<filename>"`.
`bbox_json` = `[x1,y1,x2,y2]` in page pixel coordinates at 200 DPI.
`ocr_confidence` in [0,1]. `raw_entity_label` mapped to a dim label.
`extracted_text` = full page OCR text; `extracted_value` = matched span string.
`file_type` = `pdf` | `png`. `source_format='PDF/PNG'`.
`document_key = sha256(f"unstructured|{batch_id}|{relative_file_path}")`.

### `governance` tables: see `infra/postgres/init/01_schema.sql` (exact columns).

### ML raw tables: `raw.ml_data_audit_raw`, `raw.ml_eval_slices_raw`,
`raw.ml_confusion_raw`, `raw.ml_operational_raw` — exact columns in DDL.
`eval_run_id` format `<UTC timestamp compact>_<git sha 7>` e.g. `20261126T061500Z_ab12cd3`.

### dbt model contract (Workstream B must create exactly these relations)

Schemas are set by `generate_schema_name` override: folder `staging` -> `staging`, etc.

- `staging.stg_pii_documents` (view) — normalized docs, all batches.
- `staging.stg_pii_spans` (view) — one row per span incl. span_index, start/end/value/label.
- `staging.stg_unstructured_extractions` (view) — normalized unstructured rows.
- `intermediate.int_pii_document_quality` (table) — per document: all dq flags,
  `dq_passed_flag`, `failure_reason` (one of NULL_DOCUMENT_KEY, NULL_TEXT,
  DUPLICATE_KEY, OUT_OF_BOUNDS_OFFSET, SUBSTRING_MISMATCH, UNMAPPED_ENTITY_LABEL),
  `span_count`, `entity_density`, `has_overlapping_spans`, `has_high_risk_pii`.
  Duplicate rule: `row_number() over (partition by document_key order by ingested_at desc) > 1`.
- `intermediate.int_document_risk_metrics` (table) — risk indicators used by marts.
- `intermediate.int_unstructured_quality` (table) — OCR flags + failure_reason
  (OCR_CONFIDENCE_OUT_OF_RANGE, INVALID_PAGE_NUMBER, MISSING_SOURCE_URI, UNMAPPED_ENTITY_LABEL).
- `quarantine.pii_quarantined_documents` (table): `quarantine_id` (sha256 hex),
  `document_key`, `batch_id`, `source_dataset`, `failure_reason`, `raw_payload` jsonb,
  `quarantined_at`. Must include both structured and unstructured bad rows.
- `marts.fct_pii_documents` (table, document grain, validated only):
  `document_key` (unique), `source_dataset_key`, `locale_domain_key`, `batch_id`,
  `raw_text`? (NO - keep text out of mart to stay lean; stg has it), `text_char_length`,
  `span_count`, `entity_density`, `has_high_risk_pii`, `has_overlapping_spans`,
  `dq_passed_flag`, `ingested_at`, `source_dataset`, `language`, `domain`.
- `marts.fct_pii_documents_validated` (view alias over fct_pii_documents) for PRD wording.
- `marts.fct_pii_spans` (table, span grain, validated docs only): `span_key` unique,
  `document_key`, `entity_type_key`, `raw_entity_label`, `start_offset`, `end_offset`,
  `span_value`, `offset_valid_flag`, `privacy_tier`.
- `marts.fct_unstructured_extractions` (table, field grain): `extraction_key` unique,
  `document_key`, `source_dataset_key`, `source_file_uri`, `file_type`, `page_number`,
  `bbox_json`, `ocr_confidence`, `extracted_value`, `entity_type_key`, `raw_entity_label`,
  `privacy_tier`, `has_high_risk_pii`, `needs_ocr_verification`, `extracted_text`,
  `language`, `domain`, `batch_id`, `ingested_at`.
- `marts.dim_entity_type` (from seed) — `entity_type_key`, `raw_entity_label`,
  `canonical_entity_label`, `privacy_tier` in
  (`direct_identifier`,`sensitive_phi_financial`,`credential_secret`,`quasi_identifier`),
  `is_high_risk`, `description`.
- `marts.dim_source_dataset`, `marts.dim_locale_domain`, `marts.dim_edge_case_rule`.
- `marts.bridge_record_edge_cases` (document_key, rule_id) — derived flags.
- `marts.vw_agent_triage_candidates` (view) with columns:
  `candidate_key, candidate_type` (`OCR_VERIFICATION`,`REDACTION_REVIEW`,`SLICE_QUARANTINE_ALERT`,`VERIFIED_FALSE_POSITIVE`),
  `document_key, source_file_uri, batch_id, priority, entity_type, ocr_confidence,
  entity_density, has_overlapping_spans, quarantine_rate, evidence_json` (jsonb).
  Logic: high-risk doc + (overlap or density>2.5); OCR conf<0.8 + high risk;
  batch quarantine rate 1%-5% concentrated in one entity; low-risk clean spans (conf>=0.9)
  for HOLD_AND_LOG.
- `marts.fct_ml_eval_slices`, `marts.fct_ml_data_audit`, `marts.fct_ml_confusion`,
  `marts.fct_ml_operational` from raw ML tables.

`dbt_project.yml` materializes staging as view, intermediate/marts as table,
quarantine as table. Vars: `batch_id` is NOT used to filter marts (all batches,
deduped by key) except tests read `var('quarantine_threshold', 0.05)`.

### dbt tests (Workstream B)

Schema tests: unique+not_null `document_key` on fct docs; unique `span_key`, not_null
`document_key`/offsets on fct spans; unique `extraction_key`; relationships
spans->docs, extractions->dim_entity_type, docs->dims.
Singular tests must exist and pass on clean data:
- `assert_no_invalid_offsets.sql`: 0 rows where offset_valid_flag = false.
- `assert_no_unmapped_entity_labels.sql`: 0 rows with null entity_type_key.
- `assert_no_quarantined_in_marts.sql`: 0 document_keys in both quarantine and fct docs.
- `assert_quarantine_rate_below_threshold.sql`: returns a row per batch where
  quarantined/total > `var('quarantine_threshold', 0.05)`. (Circuit breaker.)
- `assert_ocr_confidence_in_range.sql`: 0 rows outside [0,1] or page_number < 1.
- `assert_span_substring_alignment.sql`: 0 mismatches (re-verify in mart).
- `assert_row_grain_one_per_document.sql`: no duplicate document_key in fct docs.

`dbt_project.yml` must include `seeds` under `dbt_pii_hygiene` schema.

### Ingestion CLI contracts (Workstream A)

`python -m ingestion.download_sources [--data-dir data/raw] [--include-validation]`
- Downloads the 4 corpora with `hf download` when available; writes:
  `openpii_train.jsonl`, `openpii_validation.jsonl`, `nemotron_train.parquet`,
  `spy_medical.jsonl`, `spy_legal.jsonl`, `pii_scope/pii_scope.csv`.
- PII-SCOPE is gated: if inaccessible, print a clear message and DO NOT fabricate
  under the real name. `build_batches` then uses `ingestion.fixtures.make_pii_scope_fixture`
  (8,030 rows, CSV, same column names) and records `record_id` prefix `synth-`.

`python -m ingestion.build_batches --batch-id B [--limit-per-source N]`
- Reads raw files from `data/raw`; writes `data/staged/<B>/structured.parquet`
  (columns exactly `raw.pii_documents_raw` minus `ingested_at`) + `manifest.json`.
- Deterministic limit/sampling with seed 42. Normalizes labels to raw strings,
  computes document_key/template_hash, includes the PII-SCOPE fixture when real CSV absent.
- Prints per-source row counts and total unique document_key count (must be ≥1M in full mode).
- Handles malformed rows by skipping and counting (never crash).

`python -m ingestion.load_raw --batch-id B`
- Deletes existing rows for batch, bulk COPYs structured + unstructured parquet,
  loads `raw.edge_case_rules_raw` from `seeds/edge_case_rules.csv`, prints counts.
- `--structured-only` flag for fast iteration.

`python -m ingestion.generate_unstructured_docs --batch-id B --count 1000`
- Deterministic (seed 42). 3 doc types: clinical note, invoice, legal notice.
  60% PDF (ReportLab), 40% PNG; ~50% perturbed (rotation, blur, character confusion
  0/O, 1/l, 5/S). 1-2 pages. Writes `data/unstructured/<B>/<id>.pdf|png` +
  `manifest.json` with doc_type, domain, template_hash, language, perturbed, pages.
- Must not require the warehouse; uses local templates.

`python -m ingestion.ocr_extract_worker --batch-id B [--limit N]`
- Rasterizes PDF (PyMuPDF, 200 DPI) or opens PNG, runs pytesseract with
  `--psm 6`, builds page text + word boxes + confidences, finds candidate PII with
  deterministic recognizers (email, phone, SSN, credit card, MRN, account, date,
  names via capitalization/context), computes bbox union + mean confidence, writes
  `data/staged/<B>/unstructured.parquet` matching the raw table columns.
- `source_file_uri = local://unstructured/<B>/<filename>`; 1 row per extracted span.
  Prints files processed, rows written, mean confidence.

`python -m ingestion.inject_bad_records --batch-id test_quarantine_01 [--mode sample|flood]`
- `sample` (default): creates ~200 good docs copied from existing raw batch (or
  synthetic if DB empty) + exactly 3 bad rows: duplicate key of an existing good
  doc, out-of-bounds span (end=99999), null label (plus unstructured OCR row with
  confidence -0.25), loads batch into raw, prints the injected keys and expected reasons.
- `flood`: 10% bad rows to trip the >5% circuit breaker.
- Writes `data/staged/<B>/structured.parquet` (+ unstructured) and calls load_raw logic.

### ML eval contract (Workstream C)

`python -m ml.evaluate_pii_detectors --eval-run-id ID [--max-docs N] [--bootstrap N]`
- Reads `marts.fct_pii_documents` + `marts.fct_pii_spans` (+ unstructured for modality
  slice) from DB. If marts empty: print clear error, exit 1.
- Missingness audit per column → `raw.ml_data_audit_raw` with handling rule
  (`kept|drop|impute|missingness_indicator`).
- GroupShuffleSplit 70/15/15 grouped by `template_hash` (fallback document_key)
  BEFORE fitting; assert zero template overlap between splits, print it.
- Detectors on test split sample (default ≤4000 docs): `regex_baseline` and
  `spacy_multilingual_ner` (xx_ent_wiki_sm; graceful fallback to a deterministic
  context/lexicon detector named `contextual_lexicon_ner` if spaCy/model unavailable).
  Entity-level span matching with exact overlap (char offsets).
- Metrics per (model, slice_type, slice_value, entity_type): precision, recall, f1,
  pr_auc (span-level score; use 1.0 for regex matches else 0, document-level ranking),
  support, tp/fp/fn, 95% bootstrapped CI over documents (1000 iters default).
  Slices: `privacy_tier`, `modality` (`clean_structured_text`/`ocr_perturbed_pdf`),
  `language` (`en`/`non_en_multilingual`), plus `overall`.
- Confusion rows → `raw.ml_confusion_raw`. Latency median/p95, docs/sec, peak RSS,
  artifact size → `raw.ml_operational_raw`.
- Writes START/COMPLETE lineage + one trace id. Prints headline F1 table.

`python -m agent.triage_agent [--max-actions N]`
- Precondition: latest `governance.dbt_run_metadata.test_status` MUST be `PASS`
  (else insert one `BLOCKED_DQ_FAIL` audit row with trace id and exit 0).
- Reads `marts.vw_agent_triage_candidates` (validated views only).
- Deterministic policy exactly 4 decisions/actions:
  `OPEN_OCR_VERIFICATION_TASK` -> INSERT_REVIEW_QUEUE (task_type OCR_VERIFICATION),
  `OPEN_REDACTION_REVIEW_TASK` -> INSERT_REVIEW_QUEUE (REDACTION_REVIEW),
  `QUARANTINE_SLICE_ALERT` -> INSERT_SLICE_ALERT (SLICE_QUARANTINE_ALERT),
  `HOLD_AND_LOG` -> NO_STATE_CHANGE.
- `task_id`/action idempotency: `sha256(document_key|task_type|batch_id)[:32]`;
  duplicate insert => outcome `SKIPPED_IDEMPOTENT` (unique key in action log).
- Pydantic v2 payload validation. Allowlist of action tools only.
- Optional sandbox GitHub issue only when `SANDBOX_GITHUB_REPO` + token set
  (requests POST to api.github.com; label `pii-sandbox`); default no external calls.
- Every decision writes `governance.agent_action_log` with evidence_json, decision,
  action_taken, outcome, otel_trace_id, executed_at. Prints summary counts.

### Streamlit contract (Workstream D)

`app/streamlit_app.py`, 4 tabs, works with partially empty marts and shows helpful
"run the pipeline" message instead of crashing. Read-only connection from env,
`@st.cache_data(ttl=300)`. Tabs:
1. Pipeline Health & 1M+ Proof — count distinct document_key, per-source table,
   grain statement, MAX(ingested_at), dbt status badge, quarantine by reason,
   pipeline_runs status, refresh button.
2. Corpus Risk & ML Slice Analytics — entity density histogram, privacy tier
   distribution, quarantine counts, eval metric table + F1 bar with CI error bars,
   latency/throughput table, confusion matrix heatmap. Filters: source, language, tier.
3. High-Variety Traceability — select extraction (search/filter), show extracted
   value/label/confidence/page, and render source PDF/PNG page with bbox highlighted
   side-by-side (PyMuPDF render + PIL rectangle). Resolve `local://` against
   `PII_DATA_DIR`; if file missing, show the URI clearly.
4. Agent Action History & Review Queue — action log table with expandable evidence_json,
   decision/action/outcome/trace id; OPEN queue with Approve/Dismiss/Escalate buttons
   updating `governance.agent_review_queue.status, reviewed_by, reviewed_at, review_note`
   via the least-privilege `reviewer` role; cache clear after update.
Use `st.connection`? Plain psycopg2 is fine. Never print passwords.

Docs (Workstream D): README (quickstart, architecture diagram, 1M proof query,
demo commands, evidence section placeholders), `docs/data-dictionary.md`,
`docs/gcp-portability.md` (component matrix), `docs/snowflake-port.md` (target swap,
LATERAL FLATTEN vs UNNEST, RBAC), `docs/architecture.md`, `docs/operations.md`,
`docs/ml-evaluation.md`, `docs/agent-policy.md`, `LICENSE` (Apache-2.0),
`CONTRIBUTING.md`, `helm/` (values + README for Airflow official chart and app/worker
deployments), CI workflow (ruff + pytest + dbt parse + compose config).

### Airflow DAG (orchestrator-owned, provided)

Tasks: ensure_data → generate_unstructured → ocr_extract → load_raw → dbt_run →
dbt_test (records governance.dbt_run_metadata) → ml_eval → triage_agent → summary.
`dbt_test` raises on failure so ml_eval/agent are skipped; status FAIL still recorded.
DAG params: `batch_id`, `limit_per_source`, `doc_count`. Schedule None.

## Verification commands (each workstream)

```bash
ruff check .
python -m compileall -q <your package>
python -m pytest tests -q
# ingestion smoke (no DB for generation):
python -m ingestion.build_batches --batch-id smoke --limit-per-source 50
python -m ingestion.generate_unstructured_docs --batch-id smoke --count 5
python -m ingestion.ocr_extract_worker --batch-id smoke --limit 5
```

Report back: files created, commands run + output, open risks. Do not run
`docker compose up` or destructive DB operations; the orchestrator owns the stack.
