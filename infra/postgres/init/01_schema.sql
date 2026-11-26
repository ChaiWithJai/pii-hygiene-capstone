-- ===========================================================================
-- pii_lab local warehouse bootstrap (Postgres 16)
-- Mirrors the Snowflake layout: RAW -> STAGING -> INTERMEDIATE -> MARTS
-- plus QUARANTINE, GOVERNANCE and ML evaluation result landing tables.
-- ===========================================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS quarantine;
CREATE SCHEMA IF NOT EXISTS governance;

-- ---------------------------------------------------------------------------
-- RAW: immutable landing tables. One row per source document at document grain.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.pii_documents_raw (
    batch_id        TEXT        NOT NULL,
    document_key    TEXT        NOT NULL,
    source_dataset  TEXT        NOT NULL,
    source_format   TEXT        NOT NULL,
    record_id       TEXT        NOT NULL,
    raw_text        TEXT        NOT NULL,
    spans_json      JSONB       NOT NULL DEFAULT '[]'::jsonb,
    language        TEXT,
    domain          TEXT,
    template_hash   TEXT,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_pii_documents_raw_batch
    ON raw.pii_documents_raw (batch_id);
CREATE INDEX IF NOT EXISTS idx_pii_documents_raw_source
    ON raw.pii_documents_raw (source_dataset, source_format);
CREATE INDEX IF NOT EXISTS idx_pii_documents_raw_key
    ON raw.pii_documents_raw (document_key);

-- RAW: one row per extracted PII field from a PDF / scanned image.
CREATE TABLE IF NOT EXISTS raw.unstructured_extractions_raw (
    batch_id            TEXT        NOT NULL,
    extraction_key      TEXT        NOT NULL,
    document_key        TEXT        NOT NULL,
    source_dataset      TEXT        NOT NULL,
    source_format       TEXT        NOT NULL DEFAULT 'PDF/PNG',
    source_file_uri     TEXT        NOT NULL,
    file_type           TEXT        NOT NULL,
    page_number         INTEGER     NOT NULL,
    bbox_json           JSONB       NOT NULL,
    ocr_confidence      DOUBLE PRECISION NOT NULL,
    extracted_value     TEXT        NOT NULL,
    raw_entity_label    TEXT        NOT NULL,
    extracted_text      TEXT        NOT NULL,
    language            TEXT,
    domain              TEXT,
    template_hash       TEXT,
    is_ocr_perturbed    BOOLEAN     NOT NULL DEFAULT FALSE,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_unstructured_raw_batch
    ON raw.unstructured_extractions_raw (batch_id);
CREATE INDEX IF NOT EXISTS idx_unstructured_raw_doc
    ON raw.unstructured_extractions_raw (document_key);

-- RAW: documented, versioned domain rules surfaced in the UI / agent evidence.
CREATE TABLE IF NOT EXISTS raw.edge_case_rules_raw (
    rule_id      TEXT PRIMARY KEY,
    rule_name    TEXT NOT NULL,
    description  TEXT NOT NULL,
    severity     TEXT NOT NULL,
    pattern      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- RAW: ML evaluation outputs (written by ml/evaluate_pii_detectors.py,
-- promoted into marts by dbt models).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.ml_data_audit_raw (
    eval_run_id   TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    null_count    BIGINT NOT NULL,
    total_count   BIGINT NOT NULL,
    null_fraction DOUBLE PRECISION NOT NULL,
    handling      TEXT NOT NULL,
    noted_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.ml_eval_slices_raw (
    eval_run_id  TEXT NOT NULL,
    model_name   TEXT NOT NULL,
    split        TEXT NOT NULL,
    slice_type   TEXT NOT NULL,
    slice_value  TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    precision    DOUBLE PRECISION NOT NULL,
    recall       DOUBLE PRECISION NOT NULL,
    f1           DOUBLE PRECISION NOT NULL,
    pr_auc       DOUBLE PRECISION NOT NULL,
    support      BIGINT NOT NULL,
    tp           BIGINT NOT NULL,
    fp           BIGINT NOT NULL,
    fn           BIGINT NOT NULL,
    ci_low_f1    DOUBLE PRECISION NOT NULL,
    ci_high_f1   DOUBLE PRECISION NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_ml_slices_run
    ON raw.ml_eval_slices_raw (eval_run_id, model_name, slice_type);

CREATE TABLE IF NOT EXISTS raw.ml_confusion_raw (
    eval_run_id  TEXT NOT NULL,
    model_name   TEXT NOT NULL,
    split        TEXT NOT NULL,
    slice_type   TEXT NOT NULL,
    slice_value  TEXT NOT NULL,
    entity_type  TEXT NOT NULL,
    tp           BIGINT NOT NULL,
    fp           BIGINT NOT NULL,
    fn           BIGINT NOT NULL,
    tn           BIGINT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.ml_operational_raw (
    eval_run_id       TEXT NOT NULL,
    model_name        TEXT NOT NULL,
    median_latency_ms DOUBLE PRECISION NOT NULL,
    p95_latency_ms    DOUBLE PRECISION NOT NULL,
    docs_per_sec      DOUBLE PRECISION NOT NULL,
    peak_ram_mb       DOUBLE PRECISION NOT NULL,
    artifact_size_mb  DOUBLE PRECISION NOT NULL,
    docs_evaluated    BIGINT NOT NULL,
    evaluated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- GOVERNANCE: dbt run status, agent review queue, immutable agent action log.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS governance.dbt_run_metadata (
    run_id              TEXT PRIMARY KEY,
    batch_id            TEXT NOT NULL,
    dbt_run_started_at  TIMESTAMPTZ NOT NULL,
    dbt_run_finished_at TIMESTAMPTZ,
    test_status         TEXT NOT NULL CHECK (test_status IN ('PASS', 'FAIL', 'RUNNING')),
    tests_passed        INTEGER NOT NULL DEFAULT 0,
    tests_failed        INTEGER NOT NULL DEFAULT 0,
    failure_detail      JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS governance.agent_review_queue (
    task_id          TEXT PRIMARY KEY,
    task_type        TEXT NOT NULL CHECK (task_type IN (
                        'REDACTION_REVIEW', 'OCR_VERIFICATION', 'SLICE_QUARANTINE_ALERT')),
    document_key     TEXT NOT NULL,
    source_file_uri  TEXT,
    batch_id         TEXT NOT NULL,
    priority         TEXT NOT NULL DEFAULT 'MEDIUM',
    status           TEXT NOT NULL DEFAULT 'OPEN'
                        CHECK (status IN ('OPEN', 'APPROVED', 'DISMISSED', 'ESCALATED')),
    assigned_role    TEXT NOT NULL DEFAULT 'PRIVACY_REVIEWER',
    evidence_json    JSONB NOT NULL,
    created_by       TEXT NOT NULL DEFAULT 'pii_triage_agent',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    reviewed_by      TEXT,
    reviewed_at      TIMESTAMPTZ,
    review_note      TEXT
);

CREATE TABLE IF NOT EXISTS governance.agent_action_log (
    action_id       TEXT PRIMARY KEY,
    task_id         TEXT,
    document_key    TEXT NOT NULL,
    source_file_uri TEXT,
    batch_id        TEXT NOT NULL,
    evidence_json   JSONB NOT NULL,
    decision        TEXT NOT NULL,
    action_taken    TEXT NOT NULL,
    outcome         TEXT NOT NULL,
    otel_trace_id   TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_agent_action_idempotency
    ON governance.agent_action_log (idempotency_key);

CREATE TABLE IF NOT EXISTS governance.pipeline_runs (
    run_id      TEXT PRIMARY KEY,
    batch_id    TEXT NOT NULL,
    stage       TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    detail      JSONB
);
