# Operations runbook

This runbook covers the verified local workflow. Airflow is exposed at
<http://localhost:8088>; Streamlit is exposed at <http://localhost:8501>.

## Start and run locally

1. Copy `.env.example` to `.env` and keep `AIRFLOW_PORT=8088`.
2. Start the stack with `docker compose up -d --build`.
3. Open Airflow at <http://localhost:8088> and sign in with the local development
   credentials configured by `airflow-init` (`admin` / `admin`).
4. Trigger `pii_hygiene_pipeline` with a unique `batch_id`. For a smoke run, set
   a small `limit_per_source` and `doc_count`.
5. Follow task logs in order: `ensure_data`, `generate_unstructured`,
   `ocr_extract`, `load_raw`, `dbt_run`, `dbt_test`, `ml_eval`, `triage_agent`,
   `summary`.
6. Open Streamlit at <http://localhost:8501> after the DAG succeeds.

Do not run the agent manually after a failed dbt test. Its own precondition also
checks the latest `governance.dbt_run_metadata.test_status`, but the Airflow
dependency is the primary circuit breaker.

## Host-side commands

When running Python outside containers, use the published Postgres port:

```bash
export PII_DB_HOST=127.0.0.1
export PII_DB_PORT=5433
export PII_DB_NAME=pii_lab
export PII_DB_USER=pii_admin
export PII_DB_PASSWORD=pii_admin_pw
```

Common commands are:

```bash
python -m ingestion.build_batches --batch-id smoke --limit-per-source 50
python -m ingestion.generate_unstructured_docs --batch-id smoke --count 5
python -m ingestion.ocr_extract_worker --batch-id smoke --limit 5
python -m ingestion.load_raw --batch-id smoke
python -m ml.evaluate_pii_detectors --eval-run-id 20261126T061500Z_ab12cd3
python -m agent.triage_agent --max-actions 40
```

## Health and acceptance checks

```sql
select count(distinct document_key) as unique_documents
from marts.fct_pii_documents;

select batch_id, failure_reason, count(*)
from quarantine.pii_quarantined_documents
group by 1, 2 order by 1, 2;

select run_id, batch_id, test_status, tests_passed, tests_failed,
       dbt_run_finished_at
from governance.dbt_run_metadata
order by created_at desc limit 10;

select decision, action_taken, outcome, count(*)
from governance.agent_action_log
group by 1, 2, 3 order by 1, 2, 3;
```

The first query proves the one-row-per-source-document grain. A full run satisfies
the capstone target only when it returns at least 1,000,000 distinct keys.

## Failure handling

- **Airflow unavailable on 8088:** run `docker compose ps` and inspect
  `airflow-init`, webserver, scheduler, and warehouse logs. Confirm port 8088 is
  free and `AIRFLOW_PORT` was not overridden.
- **No DAG:** check that the scheduler is healthy and the DAG directory is mounted.
  Run `docker compose exec airflow-scheduler airflow dags list-import-errors`.
- **Raw load failure:** retain staged files, fix configuration or database access,
  and rerun the same `batch_id`; the load is batch-idempotent.
- **dbt test failure:** inspect the singular failing test and quarantine reasons.
  Do not bypass the gate. Correct upstream data/logic, rebuild, and retest.
- **Quarantine circuit breaker:** a batch exceeding the configured 5% threshold is
  expected to fail. Use `inject_bad_records --mode flood` only for demonstrations.
- **Empty app:** this is safe behavior. Complete the pipeline and refresh the app;
  cached queries expire after 300 seconds or can be cleared with its refresh control.
- **Missing source preview:** verify that `local://unstructured/...` resolves below
  `PII_DATA_DIR` and that the app can read the mounted artifact.
- **Duplicate agent retry:** the unique idempotency key should produce
  `SKIPPED_IDEMPOTENT`, not a duplicate review task.

## Backup, retention, and secrets

Generated data and local database volumes are disposable development artifacts,
but deleting them is destructive. Export required evidence before removing volumes.
Never paste passwords into logs or screenshots. Rotate any sandbox GitHub token
after a demonstration and leave it unset for normal local runs.

Kubernetes guidance is in [`../helm/README.md`](../helm/README.md). That deployment
path and all cloud deployment instructions are **UNVERIFIED**.
