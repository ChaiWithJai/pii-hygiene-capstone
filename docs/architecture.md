# Architecture

The PII Hygiene Capstone is local-first. Postgres 16 is the reference warehouse,
Apache Airflow coordinates the pipeline, dbt enforces data contracts, and Streamlit
provides the reviewer interface. Cloud targets are portability designs, not validated
deployments.

```text
Public corpora / synthetic fixtures       Synthetic PDF and PNG files
                 |                                   |
                 v                                   v
       normalize + Parquet                 OCR + PII extraction
                 \                                   /
                  v                                 v
                 Postgres RAW (immutable landing tables)
                                |
                                v
            dbt STAGING -> INTERMEDIATE -> MARTS
                        \-> QUARANTINE
                                |
                   dbt tests / circuit breaker
                         | PASS          | FAIL
                         v               v
                ML evaluation       downstream blocked
                         |
                         v
              deterministic triage agent
                         |
          governance review queue + immutable action log
                         |
                         v
                 Streamlit reviewer app
```

## Runtime flow

The `pii_hygiene_pipeline` DAG runs `ensure_data`, unstructured generation, OCR,
raw loading, dbt build and tests, ML evaluation, triage, and a final summary. A
failed dbt test records failure metadata and prevents ML evaluation and agent
execution. The DAG is manually triggered and accepts `batch_id`,
`limit_per_source`, and `doc_count` parameters.

Each processing stage is expected to be idempotent for a `batch_id`, carry an
OpenTelemetry trace ID, and emit OpenLineage START and COMPLETE events. Raw rows
are never updated in place. Reloading a batch deletes and replaces only that
batch; quarantine is rebuildable from raw data.

## Data boundaries

- `raw`: immutable-at-rest landing records, OCR fields, edge-case rules, and ML
  evaluation output.
- `staging`: normalized views, including one row per structured span.
- `intermediate`: document and extraction quality decisions and risk metrics.
- `quarantine`: rejected structured and unstructured records with a stable reason.
- `marts`: validated document, span, extraction, dimension, ML, and candidate data.
- `governance`: pipeline/test status, human review tasks, and append-only agent
  action evidence.

The validated document fact has exactly one row per `document_key`, where the key
is SHA-256 of `source_dataset|record_id`. Text remains in staging rather than the
lean document mart. The span fact is one row per detected span; unstructured
extractions retain page, bounding box, confidence, and source URI provenance.

## Security and privacy

Configuration comes from environment variables. Credentials, source datasets,
generated documents, and model binaries are not committed. The app uses the
least-privilege `reviewer` database role: read access to marts and narrowly scoped
updates to review status. The agent reads only validated candidates and can invoke
only its allowlisted actions. Optional GitHub issue creation remains disabled unless
explicit sandbox credentials are supplied.

## Portability boundary

OCI images, Parquet staging, Airflow, dbt, OpenTelemetry, and OpenLineage form the
portable layer. Warehouse-specific JSON expansion and hashing belong in dbt adapter
macros. See [GCP portability](gcp-portability.md) and
[Snowflake port](snowflake-port.md). Both cloud paths are explicitly **UNVERIFIED**.
