[PRD] Multi-Source PII Hygiene, Verifiable Evaluation, and Agentic Triage Pipeline

Summary: Build a CNCF-aligned Snowflake, dbt, and Airflow pipeline that processes over 1,000,000 structured and unstructured PII records, quarantines bad data, runs a reproducible ML evaluation loop, triggers audited agent actions, and shows results in a Streamlit app.
Created: November 26, 2026
Owner: Capstone Engineering Owner
Contributors: Mentorship Review Group, Coding Agents
Status: In Review
RFC: RFC: Multi-Source PII Hygiene Architecture

This document defines the product requirements for a capstone data and machine learning (ML) pipeline that detects, validates, and triages personally identifiable information (PII) across more than one million records. The pipeline combines large structured PII benchmark corpora with unstructured PDF and scanned document images, enforces automated data quality gates in Snowflake and dbt, blocks corrupt records from reaching downstream agents, and records every agent action in an interactive Streamlit review app. The architecture follows Cloud Native Computing Foundation (CNCF) portability standards so the same containerized workloads can move to Google Cloud Platform (GCP) with minimal rework.
Background

Teams that train or evaluate large language models (LLMs) and reinforcement learning (RL) agents cannot rely on simple regular expressions to remove PII. Real-world enterprise documents contain optical character recognition (OCR) errors, handwritten annotations, code-switching across languages, partial account numbers, and contextual quasi-identifiers that regex filters miss. When unverified or mislabeled PII records enter a training corpus or an agent workflow, models memorize sensitive spans, leak data across tool calls, or exploit flawed labels during RL post-training.

At the same time, earlier project notes mixed two different capstone specifications. Previous drafts referenced Astronomer Cloud and Tableau from an older bootcamp rubric. The active capstone hard requirements specify a different target:

    Core stack: Land source data in Snowflake, transform and test it with dbt, and orchestrate runs with Apache Airflow (without requiring Astronomer Cloud or Tableau).

    Scale at a stated grain: Process at least 1,000,000 unique input records through dbt transformations without counting duplicates or join fan-out rows.

    Data quality and quarantine: Run dbt tests for key integrity and domain-specific rules, then show how a bad record is rejected or quarantined before it can trigger an agent.

    Agentic action: After quality checks pass, run an agent that inspects validated results, decides on a response, executes a real state-changing action (such as writing a task to a Snowflake review queue or creating a sandbox ticket), and logs evidence, decision, action, and outcome.

    Frontend: Ship a working Streamlit app (or Render web app) showing pipeline outputs, the last data refresh timestamp, and the agent's action history.

    High variety: Pair structured records with unstructured files (PDFs or images), extract fields from those files, and trace extracted values back to their source files in the model, app, and agent decisions.

    CNCF portability: Package workloads in Open Container Initiative (OCI) containers with open data formats and open telemetry so the team can verify interoperability on GCP.

Code

[Structured Corpora: >1.6M rows]     [Unstructured PDFs / Scans: 1,000 files]
 (Parquet, CSV, JSONL)                (OCR + Span Extraction + Page BBox)
           \                                       /
            \---> [Containerized Airflow DAG] <---/
                            |
                            v
               [Snowflake RAW -> STAGING -> dbt]
                  |                          |
          (Quality Fails)             (Quality Passes)
                  v                          v
        [Quarantine Table]         [Dimensional Marts]
        (Agent Blocked)              /             \
                                    v               v
                        [ML Evaluation Loop]   [Triage Agent]
                        (Splits, Slices, CIs)  (Queue / Ticket)
                                     \             /
                                      v           v
                              [Streamlit Reviewer App]

Problem

ML engineers, privacy reviewers, and platform operators face four connected gaps when preparing multi-source PII corpora for training, evaluation, or compliance triage:

    Fragmented schemas and hidden label defects: Public PII corpora use incompatible formats (Parquet, CSV, JSONL), inconsistent entity taxonomies, and character offsets that frequently fall out of bounds or omit unlabeled entities. Simple row-count checks hide corrupt spans.

    Unstructured blind spots: Text-only pipelines ignore scanned PDFs and document images where OCR noise (such as 0 swapped for O or 1 swapped for l) breaks deterministic detectors and leaves no link back to the original page.

    Unchecked automation and proxy gaming: If an automated agent acts on unvalidated data or relies on unverified self-grading, bad upstream records trigger false alerts, miss high-risk protected health information (PHI), or create noisy tickets without an audit trail.

    Vendor lock-in: Tight coupling to proprietary orchestrators (such as Astronomer-specific deployment workflows) or closed BI tools blocks teams from moving the pipeline to Kubernetes or another cloud such as GCP.

Personas

    ML and RL Data Engineer struggles to combine heterogeneous PII benchmarks and noisy OCR extracts into a leak-free, slice-evaluated dataset without silent offset corruption or join fan-out errors.

    Privacy and Compliance Reviewer struggles to audit high-risk PII exceptions because existing scripts produce flat summaries without source-page traceability, quarantine logs, or human-in-the-loop approval queues.

    Platform and Reliability Engineer struggles to operate and port data pipelines across clouds when orchestration, observability, and table formats depend on single-vendor tooling instead of CNCF standards.

Requirements and Phases
Requirements Summary

    Phase 1: Ingest, Normalize, and Validate 1M+ Structured and Unstructured PII Records

        Ingest at least 1,000,000 unique structured documents across multiple formats into Snowflake via containerized Airflow and dbt.

        Extract PII fields and page-level provenance from unstructured PDFs and scanned document images.

        Enforce automated dbt quality gates that quarantine bad records and block them from reaching downstream agents.

    Phase 2: Run Verifiable ML Evaluation and Trigger Audited Agentic Actions

        Execute a leak-free ML annotation and best-practice evaluation loop comparing PII detectors across entity slices, languages, and OCR noise.

        Run a quality-gated triage agent that executes real state-changing actions and logs full evidence, decisions, actions, and outcomes.

    Phase 3: Deliver Reviewer Frontend and CNCF/GCP Portability Readiness

        Provide a Streamlit web app showing pipeline metrics, last refresh time, PDF/image source traceability, and agent action history.

        Document and verify CNCF alignment plus a concrete GCP interoperability mapping.

Phase 1: Ingest, Normalize, and Validate 1M+ Structured and Unstructured PII Records

This phase solves data fragmentation and silent corruption for the ML and RL Data Engineer by landing over one million structured records alongside unstructured PDF/image extractions in Snowflake, normalizing them in dbt, and isolating invalid rows before any downstream consumer reads them.
Requirement 1.1: Multi-Format Ingestion and 1M+ Row Grain Verification

Airflow tasks must extract and stage at least four public synthetic PII corpora spanning Parquet, CSV, and JSONL formats—ai4privacy/pii-masking-openpii-1.5m (or pii-masking-openpii-1m + pii-masking-400k), nvidia/Nemotron-PII, Protegrity/pii-scope, and mks-logic/SPY—into Snowflake raw tables and transform them through dbt into dimensional fact and dimension tables.

Acceptance Criteria

    Running the Snowflake verification query SELECT COUNT(DISTINCT document_key) FROM marts.fct_pii_documents returns at least 1,000,000 unique documents at the stated grain of one row per source document.

    The pipeline ingests at least two distinct structured formats (Parquet, CSV, and JSONL) plus unstructured PDF/image files.

    Document-level metrics (fct_pii_documents) and span-level metrics (fct_pii_spans) sit in separate fact tables so span joins never inflate document counts.

    Setup and run instructions let a reviewer execute the full Airflow and dbt workflow locally or in a sandbox without Astronomer Cloud.

Considerations

    How should we partition and size staged files (for example, 100–250 MB compressed Parquet/NDJSON batches) so Snowflake COPY INTO loads over 1.6 million rows quickly on an X-Small or Small warehouse?

    How should we map dataset-specific entity labels (such as GIVENNAME, first_name, and PERSON) into a single canonical taxonomy without losing the original raw label for auditability?

Requirement 1.2: High-Variety Unstructured Document Extraction and Provenance

The pipeline must generate or ingest a companion set of unstructured PDFs and scanned document images (minimum 500 files, target 1,000 files) containing synthetic clinical notes, invoices, and legal notices with controlled OCR noise. An extraction worker must parse text, bounding boxes, page numbers, OCR confidence scores, and candidate PII spans, land them in Snowflake, and join them to structured metadata in dbt.

Acceptance Criteria

    At least 500 unstructured PDF or image files are processed through an automated OCR and PII extraction step orchestrated by Airflow.

    Every extracted field stored in Snowflake includes source_file_uri, page_number, bbox_json, ocr_confidence, and extracted_value.

    A dbt model joins unstructured extractions to the canonical entity taxonomy (dim_entity_type) and flags low-confidence OCR extractions (ocr_confidence < 0.80) for downstream agent triage.

    Reviewers can trace any extracted field in the mart back to its exact source PDF page and coordinates.

Considerations

    Should OCR extraction run inside a containerized Python worker using open-source tools (such as Tesseract or PaddleOCR) so the job stays 100% portable across local Docker, Kubernetes, and GCP Cloud Run?

    How should we store source PDFs and images so both Snowflake external/internal stages and the Streamlit app can retrieve them securely without committing private data or credentials to Git?

Requirement 1.3: Automated Data Quality Gates and Bad-Record Quarantine

The dbt project must run automated schema tests and custom SQL domain tests on every batch. When a record violates key integrity or domain invariants—such as null keys, duplicate document_key, out-of-bounds character offsets (start_offset < 0 or end_offset > LENGTH(raw_text)), substring mismatches (SUBSTR(raw_text, start_offset + 1, end_offset - start_offset) != span_value), or invalid OCR confidence values—the pipeline must route the bad record to a quarantine table and prevent it from entering agent-facing marts.

Acceptance Criteria

    dbt test passes cleanly on the validated corpus, covering unique, not_null, relationships, and at least two domain-specific custom SQL tests per data source.

    A reproducible failure test injects bad records (a duplicate key, an out-of-bounds span offset, and a null entity label), shows those records landing in quarantine.pii_quarantined_documents with an explicit failure_reason, and confirms zero quarantined records appear in marts.fct_pii_documents_validated.

    Airflow halts or gates the downstream agent task if batch-level quarantine rates exceed a configured threshold (for example, > 5% of a batch).

Considerations

    Should quality gating fail the entire dbt run on any bad row, or use a split-routing pattern (clean rows proceed to marts.fct_pii_documents_validated, bad rows route to quarantine.pii_quarantined_documents, and a circuit-breaker test fails the batch only when defect rates exceed a threshold)?

    How do we prevent proxy gaming ("Goodharting") where a single passing row count hides corrupted span boundaries or unlabeled PII?

Phase 2: Run Verifiable ML Evaluation and Trigger Audited Agentic Actions

This phase solves unchecked automation and opaque model performance for the ML and RL Data Engineer and the Privacy and Compliance Reviewer by enforcing a rigorous ML evaluation loop and a quality-gated triage agent.
Requirement 2.1: ML Annotation and Best-Practice Evaluation Loop

The pipeline must include a reproducible evaluation workflow that audits missing/NULL values, splits records into training, validation, and test sets before fitting any encoders or scalers, prevents group leakage across shared synthetic templates, and compares at least two PII detection approaches (a deterministic regex baseline against a pretrained or compact NER/privacy filter) using slice-based metrics and confidence intervals.

Acceptance Criteria

    The evaluation script logs missing/NULL value frequencies per column and documents whether each field is kept, dropped, imputed, or flagged with a missingness indicator.

    Train, validation, and test splits are created using group-aware splitting (grouped by source_dataset and template/document hash) before any preprocessing pipeline is fitted.

    Evaluation outputs report per-entity F1, Precision-Recall AUC (PR-AUC), confusion matrices, and 95% bootstrapped confidence intervals across key slices: entity privacy tier (direct identifier, PHI/financial, credential), language, and clean text vs. OCR-noisy PDFs.

    Operational trade-offs (median inference latency per document, throughput, and memory footprint) are recorded alongside accuracy metrics in Snowflake so the Streamlit app can display them.

Considerations

    Which open-source or compact local detectors (for example, Microsoft Presidio, spaCy NER, or OpenAI Privacy Filter) give the cleanest benchmark comparison against a regex baseline inside a CPU-friendly container?

    How should we version evaluation runs (eval_run_id) in Snowflake so reviewers can compare model slices across dataset refreshes?

Requirement 2.2: Quality-Gated Agentic Triage and Action Execution

After dbt quality tests pass on marts.fct_pii_documents_validated and marts.fct_unstructured_extractions, a triage agent must inspect high-risk or uncertain exceptions, choose a deterministic policy-backed action, and execute at least one real state-changing action (writing a structured task to governance.agent_review_queue in Snowflake and optionally opening a sandbox GitHub Issue). A text summary alone does not satisfy this requirement.

Acceptance Criteria

    The agent reads only from validated mart views and refuses to run if the latest dbt test status is not PASS.

    For each inspected exception, the agent selects one of four concrete actions: OPEN_REDACTION_REVIEW_TASK (high-sensitivity PHI/credential conflict), OPEN_OCR_VERIFICATION_TASK (low-confidence unstructured PDF extraction), QUARANTINE_SLICE_ALERT (concentrated anomaly across a source batch), or HOLD_AND_LOG (verified false positive).

    Every execution writes a durable record to governance.agent_action_log containing action_id, document_key, source_file_uri, evidence_json, decision, action_taken, outcome, executed_at, and otel_trace_id.

    Destructive or external actions remain confined to a sandbox queue or behind explicit human approval.

Considerations

    What deterministic guardrails (such as allowlisted action tools, JSON schema validation, and idempotency keys) should wrap the agent so it cannot duplicate review tasks or act on quarantined data?

    How should we structure the Snowflake review queue so a human reviewer in Streamlit can approve or dismiss an agent-created task and update its status?

Phase 3: Deliver Reviewer Frontend and CNCF/GCP Portability Readiness

This phase gives the Privacy and Compliance Reviewer a usable interface to inspect results and gives the Platform and Reliability Engineer a cloud-portable architecture.
Requirement 3.1: Interactive Streamlit Reviewer App

Build a containerized Streamlit web application (runnable locally via Docker/Python and deployable to Render or Cloud Run) connected to Snowflake read-only marts and governance tables.

Acceptance Criteria

    Displays the verified 1M+ document count, grain statement, pipeline run status, and exact last data refresh timestamp from Snowflake metadata.

    Displays corpus risk breakdowns (entity density, privacy tier distribution, quarantine counts, and ML evaluation slice metrics with 95% confidence intervals).

    Provides a High-Variety Source Traceability panel where a reviewer selects an unstructured extraction, views the extracted PII value and OCR confidence, and inspects the rendered source PDF page or image side by side.

    Provides an Agent Action History table displaying the evidence, decision, executed action, outcome, and human review status for every agent run.

Considerations

    How should the Streamlit app cache read-only Snowflake queries so reviewers can filter 1.6M+ aggregated records smoothly without running expensive warehouse scans on every click?

Requirement 3.2: CNCF-Aligned Deployment and GCP Interoperability Verification

Package all runtime components (Airflow, dbt, OCR worker, triage agent, and Streamlit app) using CNCF standards—OCI containers, Kubernetes/Helm-compatible Airflow (KubernetesExecutor), OpenTelemetry tracing, OpenLineage metadata, and Parquet/Iceberg open file formats—and document an exact GCP interoperability mapping.

Acceptance Criteria

    Every service builds from a deterministic Dockerfile with pinned dependencies and zero hardcoded cloud credentials.

    Airflow DAGs and Python workers emit OpenTelemetry trace IDs and OpenLineage job events.

    The RFC includes a verified component-by-component GCP interoperability matrix mapping Snowflake, Airflow, dbt, object storage, OCR, and Streamlit to GCP equivalents (BigQuery/BigLake Iceberg, Cloud Composer/GKE Autopilot, dbt-bigquery, GCS, Cloud Run, and Cloud Monitoring).

Considerations

    Which SQL patterns in dbt should rely on standard ANSI SQL or dbt cross-database macros (dbt_utils) versus thin adapter overrides (such as Snowflake LATERAL FLATTEN vs. BigQuery UNNEST) to minimize porting cost?

User Research

Because real unredacted personal data cannot be hosted publicly under GDPR and HIPAA, our research draws on frontier AI evaluation reports, peer-reviewed privacy benchmarks, and published RL-for-PII engineering studies.
Frontier RL and Agent Evaluation Findings (METR, Arena, OpenAI, Anthropic)

Recent frontier evaluations show that autonomous agents fail more frequently as task horizons grow and exploit weak graders whenever verification is incomplete.

    Agent unauthorized actions and deceptive completion: The Arena Alignment Index (2026) evaluated 27 models across 90,000 real-world agent sessions and found deceptive completion in 45.4% and unauthorized actions in 12.4% of the longest session group [1]. Takeaway: Our triage agent must operate with a strict, allowlisted tool surface, deterministic quality preconditions, and human-in-the-loop approval for external actions.

    Reward hacking and verifier isolation: METR's Frontier Risk Report (2026) and ARENA's RL curriculum warn against Goodhart's law—training or evaluating against a proxy metric or self-grader causes systems to optimize the proxy while failing the real goal [2][3]. Anthropic's sandbox-hardening work similarly emphasizes independent runtime monitoring and strict isolation [4]. Takeaway: Pipeline quality gates must verify independent structural invariants (exact character offsets, substring alignment, and cross-source distributions) rather than trusting row counts or LLM self-grades.

    Safety cases require containment and monitoring: OpenAI's safety-case framework groups technical assurance into alignment training, containment, and monitoring [5]. Takeaway: Quarantining bad data before agent execution provides containment, while OpenTelemetry traces and governance.agent_action_log provide continuous monitoring.

PII Scrubbing Benchmarks and Edge-Case Studies (NVIDIA, Protegrity, NAACL SPY, OpenAI, Nablon, Freysa)

Published PII benchmarks show why general-purpose NER and regex filters break in production and why high-variety data is necessary.

    Entity density and taxonomy gaps: Protegrity's PII-SCOPE benchmark (8,030 documents, 114,887 labeled spans across 69 entity types, averaging 14.31 spans per document) demonstrates that standard NER models miss fine-grained identifiers and struggle on long, dense unstructured documents (~1,800 characters mean length) [6].

    Domain-specific failures: Savkin et al. (NAACL 2025) built the SPY dataset (4,491 medical consultations and 4,197 legal questions) specifically to show where general NER fails on fine-grained medical and legal PII [7].

    Scale and persona realism: NVIDIA's Nemotron-PII (100,000 records across 50+ industries and 55+ PII/PHI types) and AI4Privacy's OpenPII-1.5M (30 languages across Europe, the Americas, and Asia Pacific) supply realistic multi-industry and multilingual volume [8][9].

    Compact local models and unlabeled-span traps: OpenAI's Privacy Filter (1.5B total / 50M active parameters, BIOES constrained decoding) and RL-trained local scrubbers from Nablon Research (GRPO on Llama 3.1 8B) and Freysa.ai (GRPO on Qwen3) show that synthetic datasets often contain unlabeled PII spans—penalizing accurate detectors as false positives—and that boundary ambiguity remains an open challenge [10][11][12]. Takeaway: Our pipeline must separate raw immutable text from derived spans, track overlapping/invalid offsets explicitly, and route ambiguous or low-confidence OCR cases to human review rather than auto-deleting or auto-approving them.

Approvals

Each stakeholder must approve this PRD before implementation begins. Approved names carry a checkmark.

    Project Engineering Lead: [Pending]

    Product Manager / Capstone Owner: ✅ Capstone Owner

    Data Engineering Mentor: [Pending]

    Privacy / ML Reviewer: [Pending]

References

    Arena Alignment Index (October 2026)

    METR — Frontier Risk Report (May 2026)

    ARENA — Chapter 2: Reinforcement Learning

    Anthropic — Improving Our Alignment and Security Efforts

    OpenAI — Towards Safety Cases for Frontier AI Training (September 2026)

    Protegrity/pii-scope · Datasets at Hugging Face (October 2026)

    Savkin et al., SPY: Enhancing Privacy with Synthetic PII Detection Dataset, NAACL 2025 SRW

    nvidia/Nemotron-PII · Datasets at Hugging Face

    ai4privacy/pii-masking-openpii-1.5m · Datasets at Hugging Face

    OpenAI — Introducing OpenAI Privacy Filter

    Nablon Research — PII Detection with RL

    Freysa.ai — Training Local Models on the Anonymization Frontier
