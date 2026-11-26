"""Generate docs/evidence.md from the live local warehouse.

Every number in the report is read from Postgres at generation time; nothing is
hardcoded. Run after the pipeline (and optionally after the quarantine demos).
"""

from __future__ import annotations

import argparse
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg2

from common.config import get_settings, project_root

SECTIONS = [
    ("Validated documents by source", """
        SELECT source_dataset, source_format, COUNT(DISTINCT document_key) AS unique_documents,
               MIN(ingested_at)::date AS first_ingested, MAX(ingested_at) AS last_refreshed
        FROM marts.fct_pii_documents
        GROUP BY 1, 2 ORDER BY unique_documents DESC
    """),
    ("Grain proof (total)", """
        SELECT COUNT(*) AS rows, COUNT(DISTINCT document_key) AS unique_document_keys
        FROM marts.fct_pii_documents
    """),
    ("Span fact grain", """
        SELECT COUNT(*) AS span_rows, COUNT(DISTINCT span_key) AS unique_span_keys,
               COUNT(DISTINCT document_key) AS documents_referenced
        FROM marts.fct_pii_spans
    """),
    ("Quarantine by failure reason", """
        SELECT failure_reason, COUNT(*) AS records,
               COUNT(DISTINCT batch_id) AS batches
        FROM quarantine.pii_quarantined_documents
        GROUP BY 1 ORDER BY records DESC
    """),
    ("Quarantine leakage check (must be 0)", """
        SELECT COUNT(*) AS quarantined_keys_present_in_marts
        FROM quarantine.pii_quarantined_documents q
        JOIN marts.fct_pii_documents d USING (document_key)
    """),
    ("Unstructured extractions", """
        SELECT COUNT(*) AS extractions, COUNT(DISTINCT document_key) AS documents,
               COUNT(DISTINCT source_file_uri) AS source_files,
               ROUND(AVG(ocr_confidence)::numeric, 4) AS mean_ocr_confidence,
               SUM(CASE WHEN needs_ocr_verification THEN 1 ELSE 0 END) AS needs_verification
        FROM marts.fct_unstructured_extractions
    """),
    ("Data quality tests (latest run)", """
        SELECT run_id, batch_id, test_status, tests_passed, tests_failed,
               dbt_run_finished_at
        FROM governance.dbt_run_metadata ORDER BY created_at DESC LIMIT 5
    """),
    ("ML evaluation headline", """
        SELECT model_name, slice_type, slice_value, entity_type,
               ROUND(f1::numeric, 4) AS f1,
               ROUND(ci_low_f1::numeric, 4) AS ci_low,
               ROUND(ci_high_f1::numeric, 4) AS ci_high,
               support
        FROM marts.fct_ml_eval_slices
        WHERE slice_type = 'overall' AND model_name IS NOT NULL
        ORDER BY model_name, entity_type LIMIT 25
    """),
    ("Operational trade-offs", """
        SELECT model_name, ROUND(median_latency_ms::numeric, 2) AS median_ms,
               ROUND(p95_latency_ms::numeric, 2) AS p95_ms,
               ROUND(docs_per_sec::numeric, 2) AS docs_per_sec,
               ROUND(peak_ram_mb::numeric, 1) AS peak_ram_mb
        FROM marts.fct_ml_operational ORDER BY model_name
    """),
    ("Agent decision + action counts", """
        SELECT decision, action_taken, outcome, COUNT(*) AS actions,
               MIN(executed_at) AS first_seen, MAX(executed_at) AS last_seen
        FROM governance.agent_action_log
        GROUP BY 1, 2, 3 ORDER BY actions DESC
    """),
    ("Review queue status", """
        SELECT task_type, status, COUNT(*) AS tasks
        FROM governance.agent_review_queue GROUP BY 1, 2 ORDER BY 1, 2
    """),
    ("Pipeline runs", """
        SELECT stage, status, COUNT(*) AS runs, MAX(finished_at) AS last_finished
        FROM governance.pipeline_runs GROUP BY 1, 2 ORDER BY 1, 2
    """),
]


def table(rows: list[tuple], headers: list[str]) -> str:
    if not rows:
        return "_No rows (run the pipeline first)._\n"
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for row in rows:
        out.append("| " + " | ".join("" if v is None else str(v) for v in row) + " |")
    return "\n".join(out) + "\n"


def query(conn: Any, sql: str) -> tuple[list[str], list[tuple]]:
    with conn.cursor() as cur:
        cur.execute(sql)
        headers = [d[0] for d in cur.description]
        return headers, cur.fetchall()


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], cwd=project_root(), text=True
        ).strip()
    except Exception:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="docs/evidence.md")
    args = parser.parse_args()
    settings = get_settings()
    conn = psycopg2.connect(settings.dsn)
    lines = [
        "# Verification evidence — local pipeline run",
        "",
        f"Generated: {datetime.now(UTC).isoformat()}  ",
        f"Git commit: `{git_sha()}`  ",
        f"Warehouse: `{settings.db_name}` on `{settings.db_host}:{settings.db_port}`  ",
        "Method: every table below is the direct result of a live SQL query against the "
        "local warehouse. Regenerate with `make evidence`.",
        "",
    ]
    try:
        for title, sql in SECTIONS:
            lines.append(f"## {title}")
            lines.append("")
            try:
                headers, rows = query(conn, sql)
                lines.append(table(rows, headers))
            except Exception as error:  # noqa: BLE001 - report missing tables honestly
                conn.rollback()
                lines.append(f"_Query failed: {type(error).__name__}: {error}_\n")
            lines.append("")
    finally:
        conn.close()

    path = Path(args.output)
    if not path.is_absolute():
        path = project_root() / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {path}")


if __name__ == "__main__":
    main()
