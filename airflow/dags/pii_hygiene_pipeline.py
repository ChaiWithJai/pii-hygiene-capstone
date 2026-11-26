"""End-to-end PII hygiene DAG.

Extract -> generate unstructured -> OCR -> load raw -> dbt run -> dbt test
-> ML evaluation -> quality-gated triage agent.

LocalExecutor on Docker Compose; the same DAG runs unchanged on Kubernetes
(KubernetesExecutor) and Cloud Composer. No Astronomer dependency.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR = os.environ.get("PII_PROJECT_DIR", "/opt/pii")
DATA_DIR = os.environ.get("PII_DATA_DIR", f"{PROJECT_DIR}/data")
DBT_DIR = f"{PROJECT_DIR}/dbt_pii_hygiene"
QUARANTINE_THRESHOLD = os.environ.get("PII_QUARANTINE_THRESHOLD", "0.05")
TRACE_ENV = {"PII_TRACE_ID": "{{ run_trace_id(run_id) }}"}


def run_trace_id(run_id: str) -> str:
    return uuid5(NAMESPACE_URL, f"pii_hygiene_pipeline/{run_id}").hex


def _python(module: str) -> str:
    return f"cd {PROJECT_DIR} && {sys.executable} -m {module}"


def _dbt_binary() -> str:
    configured = os.environ.get("DBT_BINARY")
    if configured and Path(configured).exists():
        return configured
    candidate = Path("/home/airflow/dbt-venv/bin/dbt")
    if candidate.exists():
        return str(candidate)
    return shutil.which("dbt") or "dbt"


def _write_status(
    run_id: str,
    batch_id: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    exit_code: int,
    output_tail: str,
    tests_passed: int = 0,
    tests_failed: int = 0,
) -> None:
    from common.db import connect
    from common.telemetry import traced

    with traced(  # noqa: SIM117
        "airflow.dbt_status", trace_id=run_trace_id(run_id),
        attributes={"batch_id": batch_id, "status": status},
    ):
        with connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO governance.dbt_run_metadata (
                        run_id, batch_id, dbt_run_started_at, dbt_run_finished_at,
                        test_status, tests_passed, tests_failed, failure_detail
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (run_id) DO UPDATE SET
                        test_status = EXCLUDED.test_status,
                        dbt_run_finished_at = EXCLUDED.dbt_run_finished_at,
                        tests_passed = EXCLUDED.tests_passed,
                        tests_failed = EXCLUDED.tests_failed,
                        failure_detail = EXCLUDED.failure_detail
                    """,
                    (
                        run_id,
                        batch_id,
                        started_at,
                        finished_at,
                        status,
                        tests_passed,
                        tests_failed,
                        json.dumps({"exit_code": exit_code, "output_tail": output_tail}),
                    ),
                )
            conn.commit()


def run_dbt_test(**context: object) -> None:
    params = context["params"]  # type: ignore[index]
    batch_id = str(params["batch_id"])
    started_at = datetime.now(UTC)
    cmd = [
        _dbt_binary(),
        "test",
        "--project-dir",
        DBT_DIR,
        "--profiles-dir",
        DBT_DIR,
        "--vars",
        json.dumps({"quarantine_threshold": float(QUARANTINE_THRESHOLD)}),
    ]
    result = subprocess.run(
        cmd, cwd=PROJECT_DIR, capture_output=True, text=True, check=False
    )
    print(result.stdout[-12000:])
    print(result.stderr[-4000:], file=sys.stderr)
    finished_at = datetime.now(UTC)
    status = "PASS" if result.returncode == 0 else "FAIL"
    artifact = Path(DBT_DIR) / "target" / "run_results.json"
    results = []
    if artifact.exists() and artifact.stat().st_mtime >= started_at.timestamp():
        results = json.loads(artifact.read_text(encoding="utf-8")).get("results", [])
    _write_status(
        str(context["run_id"]),
        batch_id,
        status,
        started_at,
        finished_at,
        result.returncode,
        (result.stdout + "\n" + result.stderr)[-4000:],
        tests_passed=sum(row["status"] == "pass" for row in results),
        tests_failed=sum(row["status"] in {"fail", "error"} for row in results),
    )
    if result.returncode != 0:
        raise AirflowException(
            "dbt tests failed. The triage agent is gated and will not run. "
            "Inspect governance.dbt_run_metadata and quarantine.pii_quarantined_documents."
        )


def ensure_batch(**context: object) -> None:
    params = context["params"]  # type: ignore[index]
    batch_id = str(params["batch_id"])
    limit = int(params["limit_per_source"])
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", batch_id):
        raise AirflowException("Invalid batch ID")
    now = datetime.now(UTC)
    _write_status(str(context["run_id"]), batch_id, "RUNNING", now, now, 0, "")
    staged = Path(DATA_DIR) / "staged" / batch_id / "structured.parquet"
    if staged.exists():
        print(f"Staged batch already present: {staged}")
        return
    cmd = [sys.executable, "-m", "ingestion.build_batches", "--batch-id", batch_id]
    if limit > 0:
        cmd += ["--limit-per-source", str(limit)]
    subprocess.run(
        cmd, cwd=PROJECT_DIR, check=True,
        env={**os.environ, "PII_TRACE_ID": run_trace_id(str(context["run_id"]))},
    )


def record_preflight_failure(context: dict) -> None:
    now = datetime.now(UTC)
    _write_status(
        str(context["run_id"]), str(context["params"]["batch_id"]), "FAIL",
        now, now, 1, f"Task failed: {context['task_instance'].task_id}",
    )


default_args = {
    "owner": "capstone",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="pii_hygiene_pipeline",
    description="1M+ PII hygiene: ingest, quarantine, ML eval, audited agent triage",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    user_defined_macros={"run_trace_id": run_trace_id},
    default_args=default_args,
    tags=["pii", "snowflake-portable", "cncf", "dbt"],
    params={
        "batch_id": Param("local_dev_001", type="string", pattern=r"^[A-Za-z0-9_-]{1,80}$"),
        "limit_per_source": Param(0, type="integer", minimum=0),
        "doc_count": Param(1000, type="integer", minimum=1),
        "max_agent_actions": Param(40, type="integer", minimum=1),
        "eval_max_docs": Param(4000, type="integer", minimum=1),
    },
    doc_md=__doc__,
) as dag:
    ensure_data = PythonOperator(
        task_id="ensure_structured_batch",
        python_callable=ensure_batch,
        on_failure_callback=record_preflight_failure,
    )

    generate_docs = BashOperator(
        task_id="generate_unstructured_docs",
        bash_command=(
            f"{_python('ingestion.generate_unstructured_docs')} "
            "--batch-id {{ params.batch_id }} --count {{ params.doc_count }}"
        ),
        env=TRACE_ENV,
        append_env=True,
        on_failure_callback=record_preflight_failure,
    )

    ocr_extract = BashOperator(
        task_id="ocr_extract",
        bash_command=(
            f"{_python('ingestion.ocr_extract_worker')} "
            "--batch-id {{ params.batch_id }}"
        ),
        env=TRACE_ENV,
        append_env=True,
        on_failure_callback=record_preflight_failure,
    )

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=(
            f"{_python('ingestion.load_raw')} "
            "--batch-id {{ params.batch_id }}"
        ),
        env=TRACE_ENV,
        append_env=True,
        on_failure_callback=record_preflight_failure,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_DIR} && {_dbt_binary()} seed "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR} && {_dbt_binary()} run "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        execution_timeout=timedelta(hours=2),
        append_env=True,
        on_failure_callback=record_preflight_failure,
    )

    dbt_test = PythonOperator(
        task_id="dbt_test",
        python_callable=run_dbt_test,
    )

    ml_eval = BashOperator(
        task_id="ml_evaluate_detectors",
        bash_command=(
            f"{_python('ml.evaluate_pii_detectors')} "
            "--eval-run-id eval_{{ ts_nodash }} --max-docs {{ params.eval_max_docs }}"
        ),
        execution_timeout=timedelta(hours=1),
        env=TRACE_ENV,
        append_env=True,
    )

    triage = BashOperator(
        task_id="triage_agent",
        bash_command=(
            f"{_python('agent.triage_agent')} "
            "--max-actions {{ params.max_agent_actions }}"
        ),
        env={**TRACE_ENV, "PII_BATCH_ID": "{{ params.batch_id }}"},
        append_env=True,
    )

    summary = BashOperator(
        task_id="pipeline_summary",
        bash_command=(
            f"cd {PROJECT_DIR} && {sys.executable} -m scripts.pipeline_summary "
            "--batch-id {{ params.batch_id }}"
        ),
        append_env=True,
    )

    ensure_data >> generate_docs >> ocr_extract >> load_raw >> dbt_run >> dbt_test
    dbt_test >> ml_eval >> triage >> summary
