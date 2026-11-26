"""Create and load reproducible data-quality demonstration batches."""

from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from common.config import get_settings
from common.lineage import emit_event
from common.telemetry import traced
from ingestion import compute_template_hash, document_key
from ingestion.fixtures import make_pii_scope_fixture
from ingestion.load_raw import load_batch

SOURCE_DATASET = "synthetic-injection/dq-demonstration"


def _good_rows(batch_id: str, count: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, fixture in enumerate(make_pii_scope_fixture(count)):
        record_id = f"synth-good-{index:04d}"
        rows.append(
            {
                "batch_id": batch_id,
                "document_key": document_key(SOURCE_DATASET, record_id),
                "source_dataset": SOURCE_DATASET,
                "source_format": "CSV",
                "record_id": record_id,
                "raw_text": fixture["text"],
                "spans_json": fixture["privacy_mask"],
                "language": fixture["language"],
                "domain": fixture["domain"],
                "template_hash": compute_template_hash("synthetic dq fixture"),
            }
        )
    return rows


def create_injected_rows(batch_id: str, mode: str = "sample") -> tuple[list[dict[str, Any]], list[str]]:
    """Return good and intentionally invalid structured rows plus their keys."""
    if mode not in {"sample", "flood"}:
        raise ValueError("mode must be sample or flood")
    good_count = 200 if mode == "sample" else 180
    rows = _good_rows(batch_id, good_count)
    bad_count = 3 if mode == "sample" else 20
    injected: list[str] = []
    for index in range(bad_count):
        kind = index % 3
        if kind == 0:
            bad = dict(rows[index % good_count])
            bad["record_id"] = f"synth-duplicate-{index:04d}"
            # The existing document key is intentionally retained.
        elif kind == 1:
            bad = dict(rows[index % good_count])
            bad["record_id"] = f"synth-oob-{index:04d}"
            bad["document_key"] = document_key(SOURCE_DATASET, bad["record_id"])
            bad["spans_json"] = [
                {"start": 0, "end": 99999, "value": bad["raw_text"][:8], "label": "PERSON"}
            ]
        else:
            bad = dict(rows[index % good_count])
            bad["record_id"] = f"synth-null-label-{index:04d}"
            bad["document_key"] = document_key(SOURCE_DATASET, bad["record_id"])
            span = dict(bad["spans_json"][0])
            span["label"] = None
            bad["spans_json"] = [span]
        rows.append(bad)
        injected.append(bad["document_key"])
    return rows, injected


def _bad_unstructured_row(batch_id: str) -> dict[str, Any]:
    doc_key = hashlib.sha256(f"unstructured|{batch_id}|bad-ocr.png".encode()).hexdigest()
    return {
        "batch_id": batch_id,
        "extraction_key": hashlib.sha256(f"{doc_key}|1|0".encode()).hexdigest(),
        "document_key": doc_key,
        "source_dataset": SOURCE_DATASET,
        "source_format": "PDF/PNG",
        "source_file_uri": f"local://unstructured/{batch_id}/bad-ocr.png",
        "file_type": "png",
        "page_number": 1,
        "bbox_json": [10, 10, 100, 30],
        "ocr_confidence": -0.25,
        "extracted_value": "fake@example.com",
        "raw_entity_label": "EMAIL",
        "extracted_text": "Contact fake@example.com",
        "language": "en",
        "domain": "fixture",
        "template_hash": compute_template_hash("Contact {EMAIL}"),
        "is_ocr_perturbed": True,
    }


def write_injected_batch(batch_id: str, mode: str, data_dir: Path) -> list[str]:
    """Write the demonstration Parquet files without requiring PostgreSQL."""
    rows, keys = create_injected_rows(batch_id, mode)
    staged_dir = data_dir / "staged" / batch_id
    staged_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), staged_dir / "structured.parquet", compression="zstd")
    pq.write_table(
        pa.Table.from_pylist([_bad_unstructured_row(batch_id)]),
        staged_dir / "unstructured.parquet",
        compression="zstd",
    )
    return keys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", default="test_quarantine_01")
    parser.add_argument("--mode", choices=("sample", "flood"), default="sample")
    args = parser.parse_args(argv)
    settings = get_settings()
    emit_event("ingestion.inject_bad_records", "START", outputs=["data.staged", "raw"])
    with traced("ingestion.inject_bad_records", attributes={"batch_id": args.batch_id}):
        keys = write_injected_batch(args.batch_id, args.mode, settings.data_dir)
        counts = load_batch(args.batch_id, settings.data_dir)
    print("injected keys: " + ", ".join(keys))
    print(
        "expected reasons: DUPLICATE_KEY, OUT_OF_BOUNDS_OFFSET, "
        "UNMAPPED_ENTITY_LABEL, OCR_CONFIDENCE_OUT_OF_RANGE"
    )
    emit_event(
        "ingestion.inject_bad_records",
        "COMPLETE",
        outputs=["data.staged", "raw"],
        facets={**counts, "bad_structured": len(keys)},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
