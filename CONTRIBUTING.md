# Contributing

Thank you for improving the PII Hygiene Capstone. The project is local-first and treats
data quality, reproducibility, PII safety, and evidence-backed claims as part of the code
contract.

## Before you start

- Read `docs/internal/SPEC.md`; it is the source of truth for public CLIs, table names,
  columns, paths, and cross-component contracts.
- Keep changes scoped. Coordinate any shared-contract change with all affected owners.
- Use Python 3.12. Public functions require type hints and docstrings.
- Do not commit `.env`, downloaded datasets, generated Parquet/PDF/PNG files, model
  binaries, database dumps, access tokens, or real PII.
- Do not mutate `raw.*` rows in place. Pipeline stages must be idempotent by `batch_id`.
- Preserve trace IDs and START/COMPLETE OpenLineage events in pipeline stages.
- Clearly label Snowflake, BigQuery, GCP, Kubernetes, Helm, or other cloud instructions and
  results as **UNVERIFIED** unless they have corresponding reproducible cloud-run evidence.

## Set up a development environment

```bash
python3.12 -m venv .venv
.venv/bin/pip install -r requirements-worker.txt -r requirements-dbt.txt -r requirements-app.txt
cp .env.example .env
```

Start the local dependencies when database-backed work is needed:

```bash
docker compose up -d --build
docker compose ps
```

Airflow is available at <http://localhost:8088>, Streamlit at
<http://localhost:8501>, and host-side Postgres at `127.0.0.1:5433`.

## Make a change

Follow the repository contracts and keep runtime behavior offline-capable. Configuration
must come from `common.config.get_settings()` and environment variables, never embedded
secrets. When adding or changing ingestion logic, maintain deterministic fixtures and
sampling. When changing warehouse logic, preserve document grain, quarantine isolation,
and the declared dbt relations. Agent actions must remain allowlisted, idempotent, and
fully audited.

Add focused tests for new behavior. Unit tests must run without Docker; integration tests
must skip cleanly when their database is unavailable.

## Validate locally

Run the checks relevant to the change, and report commands plus outcomes:

```bash
.venv/bin/ruff check .
.venv/bin/python -m pytest tests -q
.venv/bin/dbt parse --project-dir dbt_pii_hygiene --profiles-dir dbt_pii_hygiene --target postgres
docker compose config --quiet
```

For ingestion changes, use the no-database smoke path:

```bash
.venv/bin/python -m ingestion.build_batches --batch-id smoke --limit-per-source 50
.venv/bin/python -m ingestion.generate_unstructured_docs --batch-id smoke --count 5
.venv/bin/python -m ingestion.ocr_extract_worker --batch-id smoke --limit 5
```

Generated smoke data belongs under `data/` and must not be committed. Do not claim the
smoke run demonstrates the 1M-document target.

For an already populated local warehouse, run:

```bash
PII_DB_HOST=127.0.0.1 PII_DB_PORT=5433 .venv/bin/python -m scripts.verify_acceptance
make evidence
```

The generated `docs/evidence.md` is the appropriate source for measured local results.
Never replace missing evidence with estimated numbers.

## Documentation and pull requests

Document user-visible CLI, schema, configuration, and operational changes. A proposed
change should explain:

- the problem and chosen approach;
- contracts or schemas affected;
- checks executed and their results;
- migration or rollback considerations;
- data/privacy risks and mitigations;
- remaining limitations, including **UNVERIFIED** cloud behavior.

Keep commits focused and do not include generated artifacts. Reviewers should be able to
reproduce every claimed result locally from the commands in the change description.

## Security and privacy

Use only public datasets under their applicable terms or synthetic fixtures. Never paste
real credentials or real personal information into issues, logs, fixtures, screenshots, or
test failures. If you discover an exposed secret or real PII, do not redistribute it;
privately notify the repository maintainers and rotate or remove the affected material.

By contributing, you agree that your contributions are licensed under the Apache License
2.0 in `LICENSE`.
