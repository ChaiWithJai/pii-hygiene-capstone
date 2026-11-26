# Data dictionary

All offsets are zero-based and end-exclusive. JSON types below are Postgres types;
ports must preserve the same logical shape and nullability. The DDL in
`infra/postgres/init/01_schema.sql` is authoritative for raw and governance tables.

## Raw landing tables

### `raw.pii_documents_raw`

Grain: one source document per row before quality filtering.

| Column | Type | Meaning |
|---|---|---|
| `batch_id` | text, required | Idempotent load boundary. |
| `document_key` | text, required | SHA-256 of `source_dataset|record_id`. |
| `source_dataset` | text, required | One of the four contracted corpus identifiers. |
| `source_format` | text, required | `JSONL`, `PARQUET`, or `CSV`. |
| `record_id` | text, required | Stable source-row identifier. |
| `raw_text` | text, required | Complete source document text. |
| `spans_json` | jsonb, required | Array of `{start,end,value,label}` objects. |
| `language`, `domain` | text | Slice and filtering attributes. |
| `template_hash` | text | 32-character hash used for leakage-free grouped splitting. |
| `ingested_at` | timestamptz, required | Warehouse load time. |

Contracted `source_dataset` values are
`ai4privacy/pii-masking-openpii-1.5m`, `nvidia/Nemotron-PII`,
`Protegrity/pii-scope`, and `mks-logic/SPY`.

### `raw.unstructured_extractions_raw`

Grain: one extracted PII field on one document page.

| Column | Type | Meaning |
|---|---|---|
| `batch_id` | text, required | Load boundary. |
| `extraction_key` | text, required | SHA-256 of `document_key|page_number|span_index`. |
| `document_key` | text, required | SHA-256 of `unstructured|batch_id|relative_file_path`. |
| `source_dataset` | text, required | Synthetic/unstructured source identifier. |
| `source_format` | text, required | `PDF/PNG`. |
| `source_file_uri` | text, required | `local://unstructured/<batch>/<filename>`. |
| `file_type` | text, required | `pdf` or `png`. |
| `page_number` | integer, required | One-based page number. |
| `bbox_json` | jsonb, required | `[x1,y1,x2,y2]` pixels at 200 DPI. |
| `ocr_confidence` | double, required | OCR confidence in `[0,1]`. |
| `extracted_value` | text, required | Matched field value. |
| `raw_entity_label` | text, required | Input label mapped through entity taxonomy. |
| `extracted_text` | text, required | Full OCR text for the page. |
| `language`, `domain`, `template_hash` | text | Slice and grouping attributes. |
| `is_ocr_perturbed` | boolean, required | Whether controlled OCR noise was applied. |
| `ingested_at` | timestamptz, required | Warehouse load time. |

### Other raw tables

- `raw.edge_case_rules_raw`: versioned `rule_id`, name, description, severity,
  optional pattern, and creation time.
- `raw.ml_data_audit_raw`: per-run/table/column missingness and the handling rule
  (`kept`, `drop`, `impute`, or `missingness_indicator`).
- `raw.ml_eval_slices_raw`: precision, recall, F1, PR-AUC, support, TP/FP/FN, and
  bootstrapped F1 interval by model, split, slice, and entity.
- `raw.ml_confusion_raw`: TP/FP/FN/TN at the same evaluation grain.
- `raw.ml_operational_raw`: median and p95 latency, documents/second, peak RAM,
  artifact size, and evaluated count.

## Transformation and mart relations

| Relation | Grain and purpose |
|---|---|
| `staging.stg_pii_documents` | Normalized source documents across all batches. |
| `staging.stg_pii_spans` | One row per structured span, including index and offsets. |
| `staging.stg_unstructured_extractions` | Normalized OCR extraction records. |
| `intermediate.int_pii_document_quality` | One row per candidate document with flags, failure reason, span count, density, overlap, and high-risk indicators. |
| `intermediate.int_document_risk_metrics` | Document-level risk indicators used by marts. |
| `intermediate.int_unstructured_quality` | One row per extraction with OCR validity and failure reason. |
| `quarantine.pii_quarantined_documents` | One rejected record with reason and JSON payload. Includes structured and unstructured failures. |
| `marts.fct_pii_documents` | One validated, deduplicated document; excludes raw text. |
| `marts.fct_pii_documents_validated` | View alias for the validated document fact. |
| `marts.fct_pii_spans` | One span belonging to a validated document. |
| `marts.fct_unstructured_extractions` | One validated extracted field with full page provenance. |
| `marts.dim_entity_type` | Raw-to-canonical label, privacy tier, risk flag, description. |
| `marts.dim_source_dataset` | Source dataset attributes. |
| `marts.dim_locale_domain` | Language/domain attributes. |
| `marts.dim_edge_case_rule` | Versioned edge-case rule dimension. |
| `marts.bridge_record_edge_cases` | Document-to-edge-case-rule relationship. |
| `marts.vw_agent_triage_candidates` | Validated, policy-ready candidate evidence. |
| `marts.fct_ml_*` | dbt-promoted evaluation audit, slice, confusion, and operational facts. |

Quality failure reasons are `NULL_DOCUMENT_KEY`, `NULL_TEXT`, `DUPLICATE_KEY`,
`OUT_OF_BOUNDS_OFFSET`, `SUBSTRING_MISMATCH`, `UNMAPPED_ENTITY_LABEL`,
`OCR_CONFIDENCE_OUT_OF_RANGE`, `INVALID_PAGE_NUMBER`, and `MISSING_SOURCE_URI`.

Entity privacy tiers are `direct_identifier`, `sensitive_phi_financial`,
`credential_secret`, and `quasi_identifier`.

## Governance tables

- `governance.dbt_run_metadata`: a run's batch, timestamps, `PASS`/`FAIL`/`RUNNING`
  test status, test counts, and JSON failure detail.
- `governance.agent_review_queue`: human task identified by `task_id`; task type,
  document/source/batch, priority, evidence, status, reviewer, timestamp, and note.
  Status is `OPEN`, `APPROVED`, `DISMISSED`, or `ESCALATED`.
- `governance.agent_action_log`: immutable evidence, policy decision, action,
  outcome, trace ID, idempotency key, and execution time for every agent decision.
- `governance.pipeline_runs`: per-stage run status and optional JSON detail.
