[RFC] Multi-Source PII Hygiene, Verifiable Evaluation, and Agentic Triage Architecture

Summary: Design a CNCF-aligned Snowflake, dbt, and Apache Airflow pipeline that processes over 1.6 million structured and unstructured PII records, enforces quarantine quality gates, runs a leak-free ML evaluation loop, executes audited agent actions, renders results in Streamlit, and documents a verified GCP interoperability path.
Created: November 26, 2026
Current Version: 0.1.0
Target Version: 1.0.0
PRD: PRD: Multi-Source PII Hygiene Pipeline
Status: In Review

Owner: Capstone Engineering Owner
Contributors: Mentorship Review Group, Coding Agents
Other stakeholders: Data Engineering Mentor, Privacy / ML Reviewer
Approvers: Capstone Engineering Owner, Data Engineering Mentor

This RFC specifies the technical architecture, data model, data quality gates, ML evaluation loop, agentic action design, and Streamlit interface for the Multi-Source PII Hygiene Capstone Pipeline. The design fulfills every hard requirement of the Snowflake + dbt capstone—processing over 1,000,000 unique documents at a stated grain, combining structured records with unstructured PDFs and scanned document images, quarantining invalid rows before agent execution, executing real state-changing agent actions, and shipping an interactive reviewer frontend.

All services run in Open Container Initiative (OCI) containers orchestrated by Apache Airflow with OpenTelemetry and OpenLineage instrumentation. This CNCF-aligned design eliminates Astronomer Cloud and Tableau lock-in while allowing the team to port the entire workload to Google Cloud Platform (GCP) by swapping only the warehouse and managed-runtime adapters.
Background

Preparing multi-source personally identifiable information (PII) corpora for LLM pretraining, reinforcement learning (RL) post-training, or compliance review fails when pipelines treat redaction as a single regex pass or trust raw benchmark schemas blindly. Public synthetic PII corpora (ai4privacy/pii-masking-openpii-1.5m, nvidia/Nemotron-PII, Protegrity/pii-scope, and mks-logic/SPY) ship in different file formats (Parquet, CSV, JSONL), encode spans using incompatible conventions (character offsets vs. token-level BIO tags), and sometimes contain out-of-bounds indices or unlabeled entities. Meanwhile, enterprise documents frequently arrive as scanned PDFs or images where optical character recognition (OCR) noise breaks exact string matching and obscures source-page provenance.

Earlier project notes conflated an older bootcamp specification (which required Astronomer Cloud and Tableau) with the active capstone hard requirements. The active requirements center on Snowflake, dbt, at least 1,000,000 input records at a stated grain, automated dbt data quality tests with a bad-record quarantine demonstration, a quality-gated agent that executes a real action and logs evidence/decision/action/outcome, a Streamlit or Render web app, and either high variety (unstructured files extracted and traced back to source) or high velocity. This RFC resolves that ambiguity by selecting High Variety—pairing >1.6 million structured records with 1,000 synthetic OCR-perturbed PDFs and scanned images—while using containerized Apache Airflow and CNCF standards so the stack remains portable across Snowflake and GCP.
Proposal

We propose a five-layer, containerized architecture:

    Containerized Extraction and OCR Layer (Airflow + Python Workers): Apache Airflow (running on Docker Compose locally or Kubernetes via the official Helm chart and KubernetesExecutor) orchestrates PEP8-linted Python workers that pull structured PII datasets in Parquet, CSV, and JSONL formats and run open-source OCR (PaddleOCR / Tesseract) plus baseline PII extraction over 1,000 synthetic PDFs and scanned document images. Workers write compressed 100–250 MB Parquet/NDJSON batches to cloud object storage or a Snowflake stage.

    Layered Snowflake + dbt Warehouse (RAW -> STAGING -> INTERMEDIATE -> MARTS + QUARANTINE): Raw text and VARIANT span arrays land immutably in RAW. dbt staging models flatten spans via LATERAL FLATTEN, validate character offsets (0 <= start_offset < end_offset <= LENGTH(raw_text)) and substring alignment, and route invalid records into quarantine.pii_quarantined_documents while promoting verified records into separated dimensional fact tables (fct_pii_documents, fct_pii_spans, and fct_unstructured_extractions).

    Verifiable ML Evaluation Loop: A containerized evaluation job reads from validated marts, audits missing/NULL values, performs group-aware train/validation/test splits before fitting encoders or scalers, compares a deterministic regex baseline against a local NER/privacy filter (Microsoft Presidio / OpenAI Privacy Filter), and writes per-entity F1, PR-AUC, 95% bootstrapped confidence intervals, confusion matrices, and latency metrics back to Snowflake (marts.fct_ml_eval_slices).

    Quality-Gated Triage Agent: Triggered only after dbt test exits with status 0 and batch quarantine rates stay below 5%, the triage agent queries validated exception views, applies an allowlisted decision policy, executes real state-changing actions (writing tasks to governance.agent_review_queue in Snowflake and optionally opening a sandbox GitHub Issue), and records full evidence, decision, action, outcome, and otel_trace_id in governance.agent_action_log.

    Interactive Streamlit Reviewer App and CNCF Observability: A containerized Streamlit app (runnable locally or deployed on Render / Cloud Run) surfaces the 1M+ row proof, last data refresh timestamp, ML slice metrics, side-by-side PDF/image source traceability, and the interactive agent review queue. OpenTelemetry traces and OpenLineage events span Airflow, dbt, OCR, and the agent.

Abandoned Ideas

Recording rejected designs prevents future contributors from revisiting dead ends:

    Astronomer Cloud deployment workflow: Abandoned because the active capstone requirements do not require Astronomer, and tying CI/CD to a shared .astro/config.yml deployment introduces vendor lock-in and overwrite hazards. Containerized open-source Apache Airflow with Helm/KubernetesExecutor preserves CNCF portability.

    Tableau as the primary frontend: Abandoned because Tableau requires proprietary licensing, cannot easily render inline source PDF pages beside bounding-box metadata, and does not support interactive human-in-the-loop approval buttons for an agent review queue. Streamlit satisfies the capstone frontend requirement directly and deploys cleanly in a container.

    Single flat PII table: Abandoned because joining document metadata directly to flattened entity spans creates many-to-many fan-out (a document with 14 spans would be counted 14 times), violating the capstone rule against inflating the 1,000,000-row count via joins.

    Naive regex-only scrubbing and LLM-as-a-judge self-grading: Abandoned because regex filters fail on OCR noise (0/O, 1/l), code-switching, and contextual identifiers, while LLM self-grading invites Goodhart's law (optimizing for the judge's blind spots instead of ground-truth spans). Independent SQL invariants and slice-based ML evaluation replace both.

    High-Velocity synthetic streaming instead of High-Variety PDFs: Abandoned as the primary capstone route because PII compliance failures in enterprise ML pipelines stem heavily from multimodal/unstructured documents (scanned clinical notes, legal PDFs, invoices). High Variety showcases unstructured extraction and source traceability directly; streaming can be added later without changing the core star schema.

System Architecture and CNCF Alignment

The pipeline separates compute, orchestration, storage, and observability along CNCF-standard boundaries so no layer locks the team into a single cloud vendor.
Code

+-------------------------------------------------------------------------+
|                    CNCF Control & Observability Plane                   |
|   OpenTelemetry Collector (Traces/Metrics)  |  OpenLineage (Marquez)    |
+-------------------------------------------------------------------------+
       ^                      ^                         ^
       |                      |                         |
+------+--------------+  +----+-----------------+  +----+-----------------+
| Apache Airflow      |  | Snowflake Warehouse  |  | Streamlit Frontend   |
| (Docker / Helm +    |  | (RAW -> STG -> INT   |  | (Docker / Render /   |
|  KubernetesExecutor)|  |  -> MARTS/QUARANTINE)|  |  Cloud Run)          |
+---------------------+  +----------------------+  +----------------------+
| 1. Extract Struct.  |  | - COPY INTO (Parquet)|  | 1. 1M+ Row Proof &   |
|    (HF Datasets)    |->| - dbt run & dbt test |->|    Last Refresh Time |
| 2. Generate/OCR     |  | - Quarantine Split   |  | 2. Corpus & ML Slice |
|    1,000 PDFs/PNGs  |  | - Validated Marts    |  |    Dashboard         |
| 3. Run dbt & Gates  |  | - Agent Review Queue |  | 3. PDF/Image Source  |
| 4. ML Eval Loop     |  | - Agent Action Log   |  |    Traceability View |
| 5. Triage Agent     |  +----------------------+  | 4. Agent Action Log  |
+---------------------+                            +----------------------+

CNCF Building Blocks

    Container Packaging (OCI): Separate Dockerfile targets for Airflow workers, dbt runner, OCR extractor, triage agent, and Streamlit UI ensure identical execution across local Docker Compose, Kubernetes, Render, and GCP Cloud Run.

    Kubernetes-Ready Orchestration: Airflow runs with the official Apache Airflow Helm chart and KubernetesExecutor (or LocalExecutor during local development), isolating heavy OCR and ML evaluation tasks in ephemeral pods with explicit CPU/memory limits.

    Open Storage Formats: All staged structured batches use compressed Apache Parquet (snappy/zstd, 100–250 MB per file). Production tables can be backed by Apache Iceberg tables in Snowflake (or BigLake Iceberg on GCP) for zero-copy cross-engine reads.

    OpenTelemetry (OTel): Every Airflow DAG run generates a root otel_trace_id propagated through Python extraction scripts, dbt invocations, and agent actions, allowing end-to-end latency and failure inspection in Prometheus/Grafana or Cloud Monitoring.

    OpenLineage: openlineage-airflow and openlineage-dbt emit column-level and dataset-level lineage events from raw stages to final marts.

Data Sources, 1M+ Row Grain Proof, and High-Variety Extraction
Verified Source Inventory
Source Dataset	Format	Stated Grain	Verified Row Count	Role in Pipeline
ai4privacy/pii-masking-openpii-1.5m (or openpii-1m + 400k)	Parquet	1 row per document	~1,500,000	Primary multilingual bulk volume (30 languages)
nvidia/Nemotron-PII	Parquet	1 row per document	100,000	Persona-grounded PII/PHI across 50+ industries, 55+ types
Protegrity/pii-scope	CSV	1 row per document	8,030	Ultra-dense stress test (14.31 spans/doc, 69 entity types)
mks-logic/SPY	JSONL	1 row per document	8,688	Medical (4,491) and legal (4,197) fine-grained PII benchmark
Synthetic Unstructured PDFs / Scanned PNGs	PDF / PNG	1 row per document page	1,000	High-variety OCR extraction with controlled noise and bbox provenance
Total Unique Input Documents	4 formats	1 row per document	~1,617,718	Exceeds 1,000,000-row requirement without join inflation
Snowflake 1,000,000+ Row Grain Verification Query

To satisfy the capstone proof requirement without counting duplicates or join fan-out rows, the pipeline runs and displays this exact Snowflake query against the validated document-grain mart:
SQL

SELECT
    source_dataset,
    source_format,
    COUNT(DISTINCT document_key) AS unique_documents,
    MIN(ingested_at)             AS first_ingested_at,
    MAX(ingested_at)             AS last_refreshed_at
FROM pii_lab.marts.fct_pii_documents
GROUP BY 1, 2
UNION ALL
SELECT
    'TOTAL_ALL_SOURCES'          AS source_dataset,
    'ALL'                        AS source_format,
    COUNT(DISTINCT document_key) AS unique_documents,
    MIN(ingested_at)             AS first_ingested_at,
    MAX(ingested_at)             AS last_refreshed_at
FROM pii_lab.marts.fct_pii_documents;

High-Variety Unstructured PDF and Image Extraction Pipeline

    Synthetic Document Generation: A deterministic generator (scripts/generate_unstructured_docs.py) renders 1,000 synthetic invoices, clinical discharge notes, and legal notices from held-out Nemotron-PII and SPY templates using ReportLab/WeasyPrint into multi-page PDFs and scanned PNGs. Half of the files receive controlled real-world perturbations: slight rotation (1–4 degrees), Gaussian scan blur, handwriting-style fonts, and character-confusion noise (0 <-> O, 1 <-> l, 5 <-> S).

    Containerized OCR and Span Extraction: An Airflow task runs a containerized Python worker using PaddleOCR (with pytesseract fallback) to extract line-level text, bounding boxes (bbox_json), page numbers, and per-token ocr_confidence, followed by a local PII extractor (Microsoft Presidio) to identify candidate spans.

    Immutable Artifact Storage and Provenance: Original PDFs and PNGs are stored in a dedicated stage/bucket (@pii_lab.raw.unstructured_stage or GCS bucket) under unstructured/<batch_id>/<doc_id>.pdf. Extracted records land in raw.unstructured_extractions_raw with source_file_uri, page_number, bbox_json, ocr_confidence, extracted_text, and extracted_spans_json, enabling exact field-to-page traceability in dbt, the agent, and Streamlit.

Data Model and Data Dictionary
Unstructured to Semi-Structured to Structured Progression

    Unstructured / Raw (RAW): Immutable landing tables (raw.pii_documents_raw, raw.unstructured_extractions_raw, raw.edge_case_rules_raw) preserve exact original raw_text, source_file_uri, and nested VARIANT span arrays. No in-place mutation occurs here.

    Semi-Structured (STAGING): dbt staging models normalize dataset-specific schemas (privacy_mask in PII-Scope, tokens/ent_tags in SPY, Parquet structs in Nemotron-PII and OpenPII) into a common envelope and explode span arrays via LATERAL FLATTEN into stg_pii_spans.

    Structured Dimensional Star Schema (INTERMEDIATE -> MARTS): Intermediate models compute document-level risk indicators, overlap flags, and offset validation checks. Marts separate document grain (fct_pii_documents), span grain (fct_pii_spans), and unstructured extraction grain (fct_unstructured_extractions), linked to conformed dimensions (dim_source_dataset, dim_entity_type, dim_locale_domain, dim_edge_case_rule) and a bridge table (bridge_record_edge_cases) to eliminate many-to-many double counting.

Core Data Dictionary
Table	Column	Snowflake Type	Nullable	Description
marts.fct_pii_documents	document_key	VARCHAR	No	Deterministic SHA-256 surrogate key (source_dataset || record_id); grain: 1 row per document
marts.fct_pii_documents	source_dataset_key	VARCHAR	No	Foreign key to dim_source_dataset
marts.fct_pii_documents	locale_domain_key	VARCHAR	No	Foreign key to dim_locale_domain
marts.fct_pii_documents	text_char_length	INTEGER	No	Character length of immutable raw_text
marts.fct_pii_documents	span_count	INTEGER	No	Total validated PII spans in the document
marts.fct_pii_documents	entity_density	FLOAT	No	Spans per 100 characters (span_count * 100.0 / NULLIF(text_char_length, 0))
marts.fct_pii_documents	has_high_risk_pii	BOOLEAN	No	True if document contains PHI, SSN, MRN, financial account, or credential secret
marts.fct_pii_documents	has_overlapping_spans	BOOLEAN	No	True if any two spans in the document overlap (a.start < b.end AND b.start < a.end)
marts.fct_pii_documents	dq_passed_flag	BOOLEAN	No	True only when all structural and offset invariants pass
marts.fct_pii_documents	ingested_at	TIMESTAMP_NTZ	No	UTC timestamp of batch ingestion (used for Streamlit last-refresh display)
marts.fct_pii_spans	span_key	VARCHAR	No	Unique span surrogate key; grain: 1 row per entity span
marts.fct_pii_spans	document_key	VARCHAR	No	Foreign key to fct_pii_documents
marts.fct_pii_spans	entity_type_key	VARCHAR	No	Foreign key to dim_entity_type (maps raw label to canonical taxonomy)
marts.fct_pii_spans	start_offset	INTEGER	No	Inclusive zero-based character start index in raw_text
marts.fct_pii_spans	end_offset	INTEGER	No	Exclusive character end index in raw_text
marts.fct_pii_spans	offset_valid_flag	BOOLEAN	No	True if 0 <= start_offset < end_offset <= text_char_length and substring matches
marts.fct_unstructured_extractions	extraction_key	VARCHAR	No	Unique key per extracted field from a PDF or image
marts.fct_unstructured_extractions	source_file_uri	VARCHAR	No	Stage or object-storage URI of the source PDF/PNG
marts.fct_unstructured_extractions	page_number	INTEGER	No	1-based page number in the source PDF (1 for single PNG)
marts.fct_unstructured_extractions	bbox_json	VARIANT	No	Normalized bounding-box coordinates [x1, y1, x2, y2] on the source page
marts.fct_unstructured_extractions	ocr_confidence	FLOAT	No	Mean OCR confidence score in [0.0, 1.0]
marts.fct_unstructured_extractions	needs_ocr_verification	BOOLEAN	No	True if ocr_confidence < 0.80 on a high-risk entity
quarantine.pii_quarantined_documents	quarantine_id	VARCHAR	No	Unique key for each rejected record
quarantine.pii_quarantined_documents	document_key	VARCHAR	Yes	Raw identifier if present
quarantine.pii_quarantined_documents	failure_reason	VARCHAR	No	Explicit invariant violated (e.g., OUT_OF_BOUNDS_OFFSET, DUPLICATE_KEY, NULL_TEXT)
quarantine.pii_quarantined_documents	quarantined_at	TIMESTAMP_NTZ	No	UTC timestamp when record was quarantined
governance.agent_review_queue	task_id	VARCHAR	No	Idempotent UUID for each agent-created review task
governance.agent_review_queue	task_type	VARCHAR	No	REDACTION_REVIEW, OCR_VERIFICATION, or SLICE_QUARANTINE_ALERT
governance.agent_review_queue	document_key	VARCHAR	No	Target document or extraction key
governance.agent_review_queue	status	VARCHAR	No	OPEN, APPROVED, DISMISSED, or ESCALATED
governance.agent_action_log	action_id	VARCHAR	No	Unique audit log identifier
governance.agent_action_log	evidence_json	VARIANT	No	Exact SQL metrics, span flags, and OCR scores inspected by the agent
governance.agent_action_log	decision	VARCHAR	No	Policy branch chosen by the agent
governance.agent_action_log	action_taken	VARCHAR	No	Concrete state change executed (e.g., INSERT_REVIEW_QUEUE, CREATE_SANDBOX_ISSUE)
governance.agent_action_log	outcome	VARCHAR	No	Execution result (SUCCESS, SKIPPED_IDEMPOTENT, BLOCKED_DQ_FAIL)
governance.agent_action_log	otel_trace_id	VARCHAR	No	OpenTelemetry trace ID linking the action back to the Airflow run
Data Quality Gates and Quarantine Architecture

To prevent Goodharting and silent corruption, every batch passes through independent structural invariants in dbt before any agent can read it.
Split-Routing Quarantine Pattern

    Stage-Level Validation: int_pii_document_quality evaluates every row against explicit boolean invariants:

        is_key_present: document_key IS NOT NULL AND raw_text IS NOT NULL AND LENGTH(TRIM(raw_text)) > 0

        is_key_unique: ROW_NUMBER() OVER (PARTITION BY document_key ORDER BY ingested_at) = 1

        are_offsets_valid: every child span satisfies 0 <= start_offset < end_offset <= LENGTH(raw_text) and SUBSTR(raw_text, start_offset + 1, end_offset - start_offset) = span_value

        are_labels_mapped: every raw_entity_label maps to a non-null canonical category in dim_entity_type

        is_ocr_valid (for unstructured rows): ocr_confidence BETWEEN 0.0 AND 1.0 AND page_number >= 1

    Routing: Rows passing all checks populate marts.fct_pii_documents (the validated agent-facing mart). Rows failing any check route to quarantine.pii_quarantined_documents with an exact failure_reason.

    dbt Tests and Circuit Breaker:

        Schema tests (unique, not_null, relationships) run on marts.fct_pii_documents, marts.fct_pii_spans, and marts.fct_unstructured_extractions.

        Custom SQL singular tests assert zero invalid offsets in marts.fct_pii_spans, zero unmapped entity labels, and batch quarantine rate < 5%. If the circuit breaker trips, dbt test exits non-zero and Airflow blocks the downstream agent task.

Reproducible Bad-Record Demonstration

To satisfy the capstone failure-proof requirement, scripts/inject_bad_records.py injects three synthetic corrupt records into a test batch:

    A duplicate document_key colliding with an existing Nemotron-PII row.

    A span with end_offset = 99999 exceeding LENGTH(raw_text).

    An unstructured PDF extraction with ocr_confidence = -0.25.

Running Airflow + dbt on this test batch demonstrates: (a) all three bad records land in quarantine.pii_quarantined_documents with explicit failure_reason codes, (b) zero bad records appear in marts.fct_pii_documents or governance.agent_review_queue, and (c) forcing a >5% bad batch halts dbt test and prevents the agent DAG task from starting.
ML Annotation and Best-Practice Evaluation Loop

Rather than treating PII detection as a black box, the pipeline embeds a reproducible ML evaluation job (ml/evaluate_pii_detectors.py) orchestrated by Airflow after dbt builds the validated marts.

    Schema and Missing/NULL Value Audit: Before modeling, the script inspects every feature column in marts.fct_pii_documents and marts.fct_unstructured_extractions, logs NULL frequencies to marts.fct_ml_data_audit, and applies documented handling rules: drop rows missing raw_text or ground-truth spans (already quarantined), impute missing domain or language with 'unknown', and create explicit boolean missingness indicator features (is_domain_missing, is_ocr_metadata_missing).

    Strict Featurization Ordering and Leakage Prevention: The script partitions documents into 70% train, 15% validation, and 15% test splits using GroupShuffleSplit grouped by template_hash (and source_dataset) before fitting any encoders, tokenizers, or numerical scalers. Grouping by template prevents synthetic template leakage between training and test sets. Preprocessing pipelines (OrdinalEncoder for privacy tiers, OneHotEncoder for low-cardinality nominal domains, StandardScaler for numerical document-length/density features) fit exclusively on the training split and transform validation and test splits independently.

    Baseline and Model Comparison: On the fixed test split, the loop evaluates:

        Baseline 1 (Naive Regex Detector): Deterministic patterns for SSNs, emails, phone numbers, and credit cards.

        Baseline 2 (Local NER / Privacy Filter): Microsoft Presidio (en_core_web_lg + recognizers) or a compact local filter (OpenAI Privacy Filter / GLiNER-PII) running inside the container.

    Metrics, Confidence Intervals, and Slice Error Analysis: Beyond headline accuracy, the script computes per-entity Precision, Recall, F1-Score, and PR-AUC, builds confusion matrices across canonical privacy tiers, and runs 1,000-iteration bootstrapping to calculate 95% confidence intervals for every key metric. Slice-based error analysis breaks performance down by:

        Privacy tier (direct_identifier, sensitive_phi_financial, credential_secret, quasi_identifier).

        Source modality (clean_structured_text vs. ocr_perturbed_pdf).

        Language slice (en vs. non_en_multilingual).

    Operational Trade-Offs: The script records median/p95 inference latency (ms/doc), throughput (docs/sec), peak RAM (MB), and model artifact size alongside predictive metrics in marts.fct_ml_eval_slices, enabling reviewers to weigh precision-recall gains against compute cost.

Agentic Action Architecture and Audit Logging

The PII Triage Agent (agent/triage_agent.py) runs as the final Airflow task only when upstream dbt test passes.
Deterministic Precondition and Policy Matrix

Before inspecting any record, the agent queries governance.dbt_run_metadata to confirm last_test_status = 'PASS'. If not, the agent writes a BLOCKED_DQ_FAIL entry to governance.agent_action_log and exits cleanly without creating tasks.

When quality gates pass, the agent queries marts.vw_agent_triage_candidates (which joins validated high-risk documents, overlapping-span anomalies, and low-confidence unstructured PDF extractions) and applies a transparent decision policy:
Inspected Condition (Evidence)	Agent Decision	Executed State-Changing Action
Unstructured PDF extraction with has_high_risk_pii = TRUE and ocr_confidence < 0.80	OPEN_OCR_VERIFICATION_TASK	Insert idempotent task into governance.agent_review_queue with source_file_uri, page_number, and bbox_json (+ optional sandbox GitHub Issue)
Validated document with has_high_risk_pii = TRUE and (has_overlapping_spans = TRUE OR entity_density > 2.5)	OPEN_REDACTION_REVIEW_TASK	Insert idempotent task into governance.agent_review_queue targeting privacy reviewer sign-off
Source batch with quarantine rate between 1% and 5% concentrated in one entity type	QUARANTINE_SLICE_ALERT	Write slice-level alert task to governance.agent_review_queue and log affected slice
Candidate triggered by low-risk entity with ocr_confidence >= 0.90 and clean spans	HOLD_AND_LOG	No queue task created; log verified false-positive rationale to governance.agent_action_log

Every execution writes an immutable audit row to governance.agent_action_log containing evidence_json, decision, action_taken, outcome, and otel_trace_id. Idempotency keys (SHA256(document_key || task_type || batch_id)) prevent duplicate tasks on Airflow retries. No external write occurs outside the Snowflake review queue or a sandbox GitHub repository.
Frontend UX and UI (Streamlit)

The containerized Streamlit application (app/streamlit_app.py) connects to Snowflake using a least-privilege read-only role for analytics views and a scoped AGENT_REVIEWER_ROLE for updating human approval status in governance.agent_review_queue. Query results use @st.cache_data(ttl=300) to avoid repeated warehouse scans.

The UI organizes information into four tabs:

    Pipeline Health and 1M+ Proof: Displays the live output of the COUNT(DISTINCT document_key) grain query, exact Last Data Refresh Timestamp (MAX(ingested_at)), latest dbt test status badge, and quarantined record count by failure_reason.

    Corpus Risk and ML Slice Analytics: Interactive filters by dataset, domain, language, and privacy tier showing entity density distributions, baseline vs. NER F1/PR-AUC comparisons with 95% bootstrapped confidence intervals, confusion matrices, and latency trade-offs.

    High-Variety Source Traceability: Side-by-side inspector where reviewers select any unstructured extraction from fct_unstructured_extractions, view the extracted PII span, canonical label, and OCR confidence score, and inspect the rendered source PDF page or PNG with the bounding box highlighted.

    Agent Action History and Review Queue: Live table of governance.agent_action_log showing timestamp, document_key, expandable evidence_json, decision, action_taken, outcome, and otel_trace_id, plus Approve and Dismiss controls that update governance.agent_review_queue.status for human-in-the-loop sign-off.

Implementation Plan and Repository Layout
Text

pii-hygiene-capstone/
├── docker-compose.yml              # Local Airflow, Postgres metadata, OTel Collector, Streamlit
├── helm/                           # CNCF Kubernetes / Airflow Helm values override
├── airflow/
│   └── dags/
│       └── pii_hygiene_pipeline.py # Extract -> OCR -> Stage -> dbt -> ML Eval -> Triage Agent
├── ingestion/
│   ├── extract_hf_corpora.py       # Parquet/CSV/JSONL extractors (PEP8 / ruff checked)
│   ├── generate_unstructured_docs.py
│   └── ocr_extract_worker.py       # Containerized PaddleOCR/Tesseract + Presidio worker
├── dbt_pii_hygiene/
│   ├── models/
│   │   ├── staging/                # stg_pii_documents, stg_pii_spans, stg_unstructured_extractions
│   │   ├── intermediate/           # int_pii_document_quality, int_document_risk_metrics
│   │   ├── marts/                  # fct_pii_documents, fct_pii_spans, fct_unstructured_extractions, dims
│   │   └── quarantine/             # pii_quarantined_documents
│   └── tests/                      # Custom SQL offset, substring, OCR, and circuit-breaker tests
├── ml/
│   └── evaluate_pii_detectors.py   # Missingness audit, GroupShuffleSplit, baselines, CIs, slices
├── agent/
│   └── triage_agent.py             # Quality-gated decision policy + Snowflake queue + audit log
├── app/
│   └── streamlit_app.py            # 4-tab reviewer frontend (runnable locally or on Render)
└── docs/
    ├── prd-pii-hygiene-capstone.md
    └── rfc-pii-hygiene-architecture.md

Local and Sandbox Execution Commands
Bash

# 1. Start containerized Airflow, OTel Collector, and Streamlit
docker compose up -d

# 2. Trigger the end-to-end Airflow DAG (Extract -> OCR -> Load -> dbt -> ML Eval -> Agent)
docker compose exec airflow-webserver airflow dags trigger pii_hygiene_pipeline

# 3. Run the bad-record quarantine verification test
python3 scripts/inject_bad_records.py --batch-id test_quarantine_01
dbt run --project-dir dbt_pii_hygiene && dbt test --project-dir dbt_pii_hygiene

# 4. Open Streamlit locally (or deploy app/ via Dockerfile to Render / Cloud Run)
streamlit run app/streamlit_app.py

GCP Interoperability Check (CNCF Portability Path)

Because every runtime service is packaged in OCI containers and instrumented with OpenTelemetry and OpenLineage, porting the capstone from Snowflake to Google Cloud Platform requires zero architectural redesign. Only the warehouse adapter and managed hosting targets change:
Capability	Primary Capstone Stack (Snowflake)	GCP Interoperability Equivalent	Portability Effort
Object Storage (Parquet + PDFs/PNGs)	Snowflake Internal/External Stage (S3/GCS)	Google Cloud Storage (GCS) buckets	Config-only (URI prefix swap)
Cloud Data Warehouse	Snowflake (RAW -> MARTS, VARIANT)	BigQuery (or BigLake + Apache Iceberg on GCS)	Adapter swap (dbt-snowflake -> dbt-bigquery)
SQL Transformations & Tests	dbt Core (dbt-snowflake)	dbt Core (dbt-bigquery)	Low: swap LATERAL FLATTEN -> UNNEST, VARIANT -> JSON via thin dbt macros
Workflow Orchestration	Containerized Apache Airflow (KubernetesExecutor)	Cloud Composer (managed Airflow on GKE) or self-hosted Airflow Helm on GKE Autopilot	Zero DAG code changes
Unstructured OCR & ML Eval Workers	OCI Container Pods (PaddleOCR / Presidio)	GKE Autopilot Pods or Cloud Run Jobs (optional swap to Document AI)	Zero container image changes
Agentic Triage Service	Containerized Python Agent writing to Snowflake	Cloud Run Job writing to BigQuery / Cloud SQL or Pub/Sub + GitHub sandbox	Thin DB connector swap
Streamlit Frontend	Docker / Render Web Service	Google Cloud Run (managed container)	Zero UI code changes
Observability & Lineage	OpenTelemetry Collector + OpenLineage	Cloud Trace / Cloud Monitoring (OTel exporter) + Dataplex Lineage (OpenLineage)	Config-only OTel endpoint swap
dbt Cross-Warehouse Portability Rule

To keep the SQL codebase portable between Snowflake and BigQuery, all date math, hashing, and type casting use dbt_utils cross-database macros. Only one macro (macros/flatten_spans.sql) branches on target.type to emit Snowflake LATERAL FLATTEN(input => spans_json) versus BigQuery UNNEST(JSON_QUERY_ARRAY(spans_json)).
Security, Privacy, and Operational Trade-Offs

    Synthetic Data Isolation and RBAC: Even though all benchmark corpora and generated PDFs are synthetic, the warehouse isolates them in a dedicated PII_LAB database with separate least-privilege roles (INGEST_ROLE, DBT_TRANSFORM_ROLE, AGENT_SERVICE_ROLE, STREAMLIT_READ_ROLE). No credentials or raw data files are committed to Git; secrets inject via environment variables or Kubernetes Secrets.

    Warehouse and File Sizing: Batching structured files into 100–250 MB compressed Parquet files lets an X-Small or Small Snowflake warehouse load ~1.6 million rows in minutes while keeping compute costs minimal.

    Known Limits of Synthetic Benchmarks: LLM-generated PII datasets occasionally omit labels on valid synthetic entities, which can understate precision during ML evaluation. Flagging high-density discrepancies for human review in governance.agent_review_queue mitigates this blind spot without hiding raw scores.

Answers to PRD Considerations

This section closes every open question raised in the PRD:

    File partitioning and sizing (PRD 1.1): Partition staged Parquet files by source_dataset/ingest_date/ and target 100–250 MB compressed file chunks so Snowflake COPY INTO parallelizes cleanly on an X-Small/Small warehouse.

    Canonical entity taxonomy mapping (PRD 1.1): Store raw_entity_label immutably in stg_pii_spans and map it via a dbt seed (seeds/entity_taxonomy_map.csv) into dim_entity_type.canonical_entity_label and privacy_tier.

    Portable OCR execution (PRD 1.2): Run open-source PaddleOCR (with pytesseract fallback) inside an OCI container so OCR runs identically on local Docker, Kubernetes, and GCP Cloud Run Jobs.

    Secure PDF/image storage (PRD 1.2): Store rendered PDFs and PNGs in a private Snowflake stage or GCS bucket, referenced only by source_file_uri in Snowflake; Streamlit fetches presigned URLs or stage streams at render time without committing binaries to Git.

    Quarantine split-routing vs. hard failure (PRD 1.3): Use split-routing (marts.fct_pii_documents for clean rows, quarantine.pii_quarantined_documents for invalid rows) combined with a 5% batch circuit breaker in dbt test so isolated corrupt rows are quarantined visibly while systemic upstream breaks halt the agent.

    Preventing proxy gaming / Goodharting (PRD 1.3): Enforce independent SQL invariants (exact span bounds, substring equality, key uniqueness, and OCR range checks) plus slice-based ML evaluation with 95% confidence intervals rather than trusting single row counts or LLM self-grades.

    CPU-friendly ML detector comparison (PRD 2.1): Compare a deterministic regex baseline against Microsoft Presidio (en_core_web_lg) or a compact local privacy filter inside a CPU container, logging both F1/PR-AUC and latency/RAM trade-offs.

    Versioning evaluation runs (PRD 2.1): Key marts.fct_ml_eval_slices by (eval_run_id, model_name, slice_type, slice_value) with evaluated_at timestamps so reviewers can track performance across refreshes.

    Agent guardrails and idempotency (PRD 2.2): Enforce a dbt test status precondition, allowlist only four deterministic actions, validate payloads against Pydantic schemas, and deduplicate inserts with SHA256(document_key || task_type || batch_id).

    Human-in-the-loop review updates (PRD 2.2): Scope a dedicated AGENT_REVIEWER_ROLE with UPDATE permission solely on governance.agent_review_queue (status, reviewed_by, reviewed_at) so Streamlit reviewers can approve or dismiss tasks without modifying mart tables.

    Streamlit query caching (PRD 3.1): Query pre-aggregated mart views and cache results with @st.cache_data(ttl=300) keyed on MAX(ingested_at) so filters stay fast and automatically invalidate after new pipeline runs.

    Cross-database SQL macros for GCP portability (PRD 3.2): Standardize on ANSI SQL and dbt_utils, isolating Snowflake-vs-BigQuery JSON flattening inside a single macros/flatten_spans.sql adapter macro.
