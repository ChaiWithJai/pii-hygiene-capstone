"""Print a compact, human-readable pipeline summary (Airflow final task)."""

from __future__ import annotations

import argparse

from common.db import connect


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", default="local_dev_001")
    args = parser.parse_args()
    with connect() as conn, conn.cursor() as cur:
        for label, sql in {
            "unique validated documents": (
                "SELECT COUNT(DISTINCT document_key) FROM marts.fct_pii_documents"
            ),
            "quarantined records": (
                "SELECT COUNT(*) FROM quarantine.pii_quarantined_documents"
            ),
            "validated spans": "SELECT COUNT(*) FROM marts.fct_pii_spans",
            "unstructured extractions": (
                "SELECT COUNT(*) FROM marts.fct_unstructured_extractions"
            ),
            "agent actions": "SELECT COUNT(*) FROM governance.agent_action_log",
            "open review tasks": (
                "SELECT COUNT(*) FROM governance.agent_review_queue WHERE status = 'OPEN'"
            ),
            "latest dbt status": (
                "SELECT test_status FROM governance.dbt_run_metadata "
                "ORDER BY created_at DESC LIMIT 1"
            ),
        }.items():
            cur.execute(sql)
            print(f"{label:32s}: {cur.fetchone()[0]}")
        cur.execute("SELECT COUNT(*) FROM raw.pii_documents_raw WHERE batch_id = %s", (args.batch_id,))
        print(f"{'raw rows in batch':32s}: {cur.fetchone()[0]}")


if __name__ == "__main__":
    main()
