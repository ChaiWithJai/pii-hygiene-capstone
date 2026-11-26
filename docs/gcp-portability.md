# GCP portability (**UNVERIFIED**)

> **Status: UNVERIFIED.** No GCP project, GKE cluster, Cloud Composer environment,
> BigQuery dataset, GCS bucket, Cloud Run service, IAM policy, or cloud cost/performance
> test was exercised for this repository. This document is a proposed portability
> design, not deployment evidence.

## Component mapping

| Local/reference component | Proposed GCP equivalent | Expected change | Verification needed |
|---|---|---|---|
| Local Parquet and PDF/PNG files | Private GCS buckets | URI and credential adapter; lifecycle policy | Upload/read round trip and signed access from app |
| Postgres warehouse | BigQuery datasets, optionally BigLake/Iceberg | dbt adapter, types, JSON expansion, grants | Full dbt build/test and 1M-grain proof |
| Apache Airflow containers | Cloud Composer or official chart on GKE | Environment variables, secrets, pod specs | Complete DAG including failure gate |
| Python OCR/ML/agent image | Cloud Run Jobs or GKE jobs | Job invocation and writable scratch storage | OCR dependencies, resources, timeout, retry behavior |
| dbt-postgres | dbt-bigquery | Profile and adapter-dispatched SQL | Parse, compile, run, and singular tests |
| Streamlit container | Cloud Run service | GCS artifact retrieval and database identity | All four tabs and review update transaction |
| OTel endpoint | Cloud Trace/Monitoring via collector | Exporter/auth configuration | Trace propagation across all stages |
| OpenLineage endpoint | Compatible hosted/self-managed backend | Endpoint and auth | START/COMPLETE event inspection |
| Local env/secrets | Secret Manager + Workload Identity | Secret references and least-privilege IAM | Deny/allow tests and rotation |

## Warehouse adapter work

Use a `bigquery` dbt target and keep relation names and model grains unchanged.
Isolate JSON array expansion behind the existing adapter macro: Postgres
`jsonb_array_elements`, Snowflake `LATERAL FLATTEN`, and BigQuery
`UNNEST(JSON_QUERY_ARRAY(...))` are not interchangeable SQL. Dispatch hashing,
timestamp conversion, JSON extraction, and safe casting where behavior differs.

Map `JSONB` to BigQuery `JSON`, `TIMESTAMPTZ` to `TIMESTAMP`, and constraints to dbt
tests because BigQuery does not enforce Postgres primary/check constraints in the
same way. Confirm byte/character indexing and substring semantics with non-ASCII
fixtures before trusting span offsets.

## Proposed deployment sequence

1. Create separate GCS locations for staged batches and source artifacts; deny
   public access and configure retention/lifecycle rules.
2. Create RAW, STAGING, INTERMEDIATE, MARTS, QUARANTINE, and GOVERNANCE datasets
   with service identities separated for ingest, transform, agent, and reviewer.
3. Configure a dbt-bigquery profile and compile all models for the BigQuery target.
4. Build and publish immutable OCI images, then run one fixture-only batch in a
   nonproduction project.
5. Exercise clean, sample-bad, and flood-bad batches. Prove that quarantine works
   and that a failed test prevents agent execution.
6. Deploy the app with read-only mart access plus narrowly scoped review updates.
7. Verify trace and lineage continuity, then measure cost, latency, quotas, and
   concurrency before increasing volume.

## Required evidence before changing status

The port remains **UNVERIFIED** until CI compiles for BigQuery and a GCP run captures:
resource identifiers and versions, dbt run/test output, 1M distinct-document query,
quarantine demonstration, blocked-agent demonstration, successful human review
update, source-artifact rendering, lineage events, and end-to-end trace IDs. Record
IAM policies with secrets redacted and include cleanup/cost evidence.
