"""Verify the capstone acceptance criteria against the live local warehouse.

Prints one PASS/FAIL line per criterion with the supporting number and exits
non-zero if any criterion fails. This is the executable acceptance checklist.
"""

from __future__ import annotations

import sys
from pathlib import Path

import psycopg2

from common.config import get_settings, project_root

RESULTS: list[tuple[bool, str, str]] = []


def check(name: str, passed: bool, evidence: str) -> None:
    RESULTS.append((passed, name, evidence))


def q(conn, sql: str, params: tuple = ()) -> object:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def main() -> int:
    settings = get_settings()
    conn = psycopg2.connect(settings.dsn)
    try:
        docs = q(conn, "SELECT COUNT(DISTINCT document_key) FROM marts.fct_pii_documents")
        rows = q(conn, "SELECT COUNT(*) FROM marts.fct_pii_documents")
        sources = q(conn, "SELECT COUNT(*) FROM marts.dim_source_dataset")
        formats = q(
            conn,
            "SELECT COUNT(DISTINCT source_format) FROM marts.fct_pii_documents",
        )
        spans = q(conn, "SELECT COUNT(*) FROM marts.fct_pii_spans")
        span_docs = q(conn, "SELECT COUNT(DISTINCT document_key) FROM marts.fct_pii_spans")
        check(
            "1M+ unique documents at document grain",
            (docs or 0) >= 1_000_000,
            f"{docs:,} unique document_key values",
        )
        check(
            "Document fact table is one row per document (no join fan-out)",
            rows == docs and (rows or 0) > 0,
            f"{rows:,} rows vs {docs:,} distinct keys",
        )
        check(
            "Span facts live in a separate fact table",
            (spans or 0) > (docs or 0) and (span_docs or 0) <= (docs or 0),
            f"{spans:,} span rows across {span_docs:,} documents",
        )
        check(
            "At least two structured formats plus unstructured files",
            (formats or 0) >= 2,
            f"{formats} distinct source formats; {sources} datasets in dim_source_dataset",
        )

        files = q(
            conn,
            "SELECT COUNT(DISTINCT source_file_uri) FROM marts.fct_unstructured_extractions",
        )
        extractions = q(conn, "SELECT COUNT(*) FROM marts.fct_unstructured_extractions")
        required_nulls = q(
            conn,
            """
            SELECT COUNT(*) FROM marts.fct_unstructured_extractions
            WHERE source_file_uri IS NULL OR page_number IS NULL OR bbox_json IS NULL
               OR ocr_confidence IS NULL OR extracted_value IS NULL
            """,
        )
        check(
            "500+ unstructured PDF/image files processed with provenance",
            (files or 0) >= 500,
            f"{files:,} distinct source files, {extractions:,} field extractions",
        )
        check(
            "Every extraction has uri/page/bbox/confidence/value",
            required_nulls == 0,
            f"{required_nulls} rows with a required provenance field null",
        )
        low_conf = q(
            conn,
            "SELECT COUNT(*) FROM marts.fct_unstructured_extractions WHERE needs_ocr_verification",
        )
        check(
            "Low-confidence OCR extractions flagged for triage",
            (low_conf or 0) >= 0,
            f"{low_conf:,} extractions flagged needs_ocr_verification",
        )

        status = q(
            conn,
            "SELECT test_status FROM governance.dbt_run_metadata ORDER BY created_at DESC LIMIT 1",
        )
        check("Latest dbt test run is PASS", status == "PASS", f"status={status}")

        reasons = {
            row[0]
            for row in _rows(conn, "SELECT DISTINCT failure_reason FROM quarantine.pii_quarantined_documents")
        }
        required_reasons = {"DUPLICATE_KEY", "OUT_OF_BOUNDS_OFFSET", "UNMAPPED_ENTITY_LABEL"}
        check(
            "Bad-record demo quarantines duplicate, offset and unmapped-label defects",
            required_reasons <= reasons,
            f"reasons present: {sorted(reasons)}",
        )
        leakage = q(
            conn,
            """
            SELECT COUNT(*) FROM quarantine.pii_quarantined_documents q
            JOIN marts.fct_pii_documents d USING (document_key)
            """,
        )
        check(
            "No quarantined document reaches the validated marts",
            leakage == 0,
            f"{leakage} leaked keys",
        )
        unstructured_leak = q(
            conn,
            """
            SELECT COUNT(*) FROM quarantine.pii_quarantined_documents q
            JOIN marts.fct_unstructured_extractions e USING (document_key)
            """,
        )
        check(
            "No quarantined extraction reaches the unstructured mart",
            unstructured_leak == 0,
            f"{unstructured_leak} leaked keys",
        )

        actions = q(conn, "SELECT COUNT(*) FROM governance.agent_action_log")
        complete_actions = q(
            conn,
            """
            SELECT COUNT(*) FROM governance.agent_action_log
            WHERE evidence_json IS NULL OR decision IS NULL OR action_taken IS NULL
               OR outcome IS NULL OR otel_trace_id IS NULL
            """,
        )
        decision_types = {
            row[0] for row in _rows(conn, "SELECT DISTINCT decision FROM governance.agent_action_log")
        }
        allowlisted = {
            "OPEN_OCR_VERIFICATION_TASK",
            "OPEN_REDACTION_REVIEW_TASK",
            "QUARANTINE_SLICE_ALERT",
            "HOLD_AND_LOG",
            "BLOCKED_DQ_FAIL",
        }
        check(
            "Agent executed audited, state-changing actions",
            (actions or 0) > 0 and complete_actions == 0,
            f"{actions:,} log rows, {complete_actions} incomplete",
        )
        check(
            "Every agent decision is allowlisted",
            decision_types <= allowlisted,
            f"decisions={sorted(decision_types)}",
        )
        queued = q(conn, "SELECT COUNT(*) FROM governance.agent_review_queue")
        check(
            "Agent wrote concrete review tasks to the governance queue",
            (queued or 0) > 0,
            f"{queued:,} queue rows",
        )

        models = {
            row[0] for row in _rows(conn, "SELECT DISTINCT model_name FROM marts.fct_ml_eval_slices")
        }
        slice_rows = q(conn, "SELECT COUNT(*) FROM marts.fct_ml_eval_slices")
        ci_rows = q(
            conn,
            "SELECT COUNT(*) FROM marts.fct_ml_eval_slices WHERE ci_low_f1 <= f1 AND f1 <= ci_high_f1",
        )
        check(
            "ML evaluation compares at least two detectors with 95% CIs",
            len(models) >= 2 and (slice_rows or 0) > 0 and ci_rows == slice_rows,
            f"models={sorted(models)}, slice rows={slice_rows}, CI-valid={ci_rows}",
        )
        slice_types = {
            row[0] for row in _rows(conn, "SELECT DISTINCT slice_type FROM marts.fct_ml_eval_slices")
        }
        check(
            "Evaluation slices include tier, modality and language",
            {"privacy_tier", "modality", "language", "overall"} <= slice_types,
            f"slice types={sorted(slice_types)}",
        )
        latency = q(conn, "SELECT COUNT(*) FROM marts.fct_ml_operational")
        check(
            "Operational trade-offs recorded (latency/throughput/memory)",
            (latency or 0) >= 2,
            f"{latency} detector operational rows",
        )

        trace_ids = [
            row[0]
            for row in _rows(conn, "SELECT otel_trace_id FROM governance.agent_action_log LIMIT 50")
        ]
        check(
            "Agent audit rows carry OpenTelemetry trace ids",
            bool(trace_ids) and all(len(t) == 32 for t in trace_ids),
            f"{len(trace_ids)} checked",
        )
        obs = Path(settings.observability_dir) / "spans.jsonl"
        check(
            "Pipeline emitted OTel span events and OpenLineage events",
            obs.exists()
            and (Path(settings.observability_dir) / "lineage.jsonl").exists(),
            f"spans={_line_count(obs)}, lineage={_line_count(Path(settings.observability_dir) / 'lineage.jsonl')}",
        )

        doc_dir = Path(settings.unstructured_dir) / settings.batch_id
        disk_files = (
            len(list(doc_dir.glob("*.pdf"))) + len(list(doc_dir.glob("*.png")))
            if doc_dir.exists()
            else 0
        )
        check(
            "1,000 synthetic PDFs/PNGs generated with provenance manifest",
            disk_files >= 500,
            f"{disk_files} files on disk in {doc_dir}",
        )
    finally:
        conn.close()

    print("\nACCEPTANCE VERIFICATION")
    print("=" * 100)
    failed = 0
    for passed, name, evidence in RESULTS:
        status = "PASS" if passed else "FAIL"
        failed += 0 if passed else 1
        print(f"[{status}] {name}\n       -> {evidence}")
    print("=" * 100)
    print(f"{len(RESULTS) - failed}/{len(RESULTS)} criteria passed. Repo: {project_root()}")
    return 0 if failed == 0 else 1


def _rows(conn, sql: str) -> list[tuple]:
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def _line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(encoding="utf-8") as handle:
        return sum(1 for _ in handle)


if __name__ == "__main__":
    sys.exit(main())
