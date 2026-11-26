"""Idempotently bulk-load staged Parquet files into PostgreSQL raw tables."""

from __future__ import annotations

import argparse
import csv
import json
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from common.config import get_settings, project_root
from common.db import connect, copy_rows, replace_batch
from common.lineage import emit_event
from common.telemetry import traced
from ingestion import STRUCTURED_COLUMNS, UNSTRUCTURED_COLUMNS


def _database_value(value: Any) -> Any:
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def parquet_rows(path: Path, columns: Sequence[str]) -> Iterator[tuple[Any, ...]]:
    """Stream ordered, COPY-safe rows from a staged Parquet file."""
    parquet = pq.ParquetFile(path)
    missing = set(columns) - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError(f"{path} is missing columns: {sorted(missing)}")
    for batch in parquet.iter_batches(columns=list(columns), batch_size=10_000):
        for row in batch.to_pylist():
            yield tuple(_database_value(row[column]) for column in columns)


def _load_edge_rules(conn: Any, seed_path: Path) -> int:
    if not seed_path.exists():
        print(f"edge case seed not present; skipped: {seed_path}")
        return 0
    with seed_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if not rows:
        return 0
    columns = ("rule_id", "rule_name", "description", "severity", "pattern")
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM raw.edge_case_rules_raw")
    return copy_rows(
        conn,
        "raw.edge_case_rules_raw",
        columns,
        (tuple(row.get(column) for column in columns) for row in rows),
    )


def load_batch(batch_id: str, data_dir: Path, structured_only: bool = False) -> dict[str, int]:
    """Replace one batch in raw tables in a single transaction."""
    staged_dir = data_dir / "staged" / batch_id
    structured_path = staged_dir / "structured.parquet"
    if not structured_path.exists():
        raise FileNotFoundError(f"missing staged structured batch: {structured_path}")
    unstructured_path = staged_dir / "unstructured.parquet"
    settings = get_settings()
    counts = {"structured": 0, "unstructured": 0, "edge_case_rules": 0}
    with connect(settings) as conn:
        replace_batch(conn, "raw.pii_documents_raw", batch_id)
        counts["structured"] = copy_rows(
            conn,
            "raw.pii_documents_raw",
            STRUCTURED_COLUMNS,
            parquet_rows(structured_path, STRUCTURED_COLUMNS),
        )
        if not structured_only:
            replace_batch(conn, "raw.unstructured_extractions_raw", batch_id)
            if unstructured_path.exists():
                counts["unstructured"] = copy_rows(
                    conn,
                    "raw.unstructured_extractions_raw",
                    UNSTRUCTURED_COLUMNS,
                    parquet_rows(unstructured_path, UNSTRUCTURED_COLUMNS),
                )
        counts["edge_case_rules"] = _load_edge_rules(
            conn, project_root() / "dbt_pii_hygiene" / "seeds" / "edge_case_rules.csv"
        )
        conn.commit()
    print(
        f"loaded structured={counts['structured']} unstructured={counts['unstructured']} "
        f"edge_case_rules={counts['edge_case_rules']}"
    )
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--structured-only", action="store_true")
    args = parser.parse_args(argv)
    emit_event("ingestion.load_raw", "START", inputs=["data.staged"], outputs=["raw"])
    with traced("ingestion.load_raw", attributes={"batch_id": args.batch_id}):
        counts = load_batch(args.batch_id, get_settings().data_dir, args.structured_only)
    emit_event(
        "ingestion.load_raw",
        "COMPLETE",
        inputs=["data.staged"],
        outputs=["raw.pii_documents_raw", "raw.unstructured_extractions_raw"],
        facets=counts,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
