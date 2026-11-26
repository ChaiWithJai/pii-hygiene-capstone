"""Normalize heterogeneous structured sources into one staged Parquet batch."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
from collections.abc import Iterable, Iterator, Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from common.config import get_settings
from common.lineage import emit_event
from common.telemetry import traced
from ingestion import (
    SOURCE_DATASETS,
    STRUCTURED_COLUMNS,
    atomic_write_json,
    compute_template_hash,
    document_key,
    mask_text_with_spans,
    normalize_spans,
)
from ingestion.fixtures import SYNTHETIC_DATASETS, make_pii_scope_fixture


def _iter_jsonl_counted(path: Path, stats: dict[str, int]) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                stats["malformed"] += 1
                continue
            if isinstance(row, Mapping):
                yield dict(row)
            else:
                stats["malformed"] += 1


def _iter_csv(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as handle:
        yield from csv.DictReader(handle)


def _iter_parquet(path: Path) -> Iterator[dict[str, Any]]:
    parquet = pq.ParquetFile(path)
    for batch in parquet.iter_batches(batch_size=10_000):
        yield from batch.to_pylist()


def _bio_spans(tokens: Any, labels: Any) -> tuple[str, list[dict[str, Any]]]:
    if not isinstance(tokens, list) or not isinstance(labels, list):
        return "", []
    text = " ".join(str(token) for token in tokens)
    spans: list[dict[str, Any]] = []
    offsets: list[tuple[int, int]] = []
    cursor = 0
    for token in tokens:
        value = str(token)
        offsets.append((cursor, cursor + len(value)))
        cursor += len(value) + 1
    active_start: int | None = None
    active_end = 0
    active_label: str | None = None
    for index, raw_label in enumerate(labels[: len(tokens)]):
        label = str(raw_label)
        prefix, _, entity = label.partition("-")
        if label == "O" or prefix not in {"B", "I"}:
            if active_start is not None:
                spans.append(
                    {"start": active_start, "end": active_end, "value": text[active_start:active_end], "label": active_label}
                )
            active_start = None
            active_label = None
            continue
        if prefix == "B" or active_start is None or entity != active_label:
            if active_start is not None:
                spans.append(
                    {"start": active_start, "end": active_end, "value": text[active_start:active_end], "label": active_label}
                )
            active_start = offsets[index][0]
            active_label = entity
        active_end = offsets[index][1]
    if active_start is not None:
        spans.append(
            {"start": active_start, "end": active_end, "value": text[active_start:active_end], "label": active_label}
        )
    return text, spans


def normalize_source_row(
    row: Mapping[str, Any],
    *,
    batch_id: str,
    source_dataset: str,
    source_format: str,
    fallback_record_id: str,
) -> dict[str, Any]:
    """Normalize one row across the known public and fixture source shapes."""
    record_id = str(row.get("uid") or row.get("id") or row.get("record_id") or fallback_record_id)
    text = (
        row.get("raw_text") or row.get("source_text") or row.get("unmasked_text")
        or row.get("text") or row.get("sentence")
    )
    raw_spans = row.get("spans_json") or row.get("privacy_mask") or row.get("spans") or row.get("entities")
    if not text and row.get("tokens") is not None:
        text, token_spans = _bio_spans(
            row.get("tokens"), row.get("ent_tags") or row.get("labels") or row.get("ner_tags")
        )
        raw_spans = raw_spans or token_spans
    if not isinstance(text, str) or not text:
        raise ValueError("row has no text")
    spans = normalize_spans(raw_spans, text)
    template = row.get("masked_text") or row.get("text_tagged") or row.get("template")
    if not isinstance(template, str) or not template:
        template = mask_text_with_spans(text, spans)
    return {
        "batch_id": batch_id,
        "document_key": document_key(source_dataset, record_id),
        "source_dataset": source_dataset,
        "source_format": source_format,
        "record_id": record_id,
        "raw_text": text,
        "spans_json": spans,
        "language": str(row.get("language") or row.get("lang") or "en"),
        "domain": str(row.get("domain") or row.get("category") or "general"),
        "template_hash": compute_template_hash(template),
    }


def _reservoir(rows: Iterable[dict[str, Any]], limit: int | None) -> list[dict[str, Any]]:
    if limit is None:
        return list(rows)
    rng = random.Random(42)
    selected: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if index < limit:
            selected.append(row)
        else:
            replacement = rng.randint(0, index)
            if replacement < limit:
                selected[replacement] = row
    return selected


def _source_inputs(raw_dir: Path) -> list[tuple[str, str, Path, str]]:
    candidates = [
        ("openpii", SOURCE_DATASETS["openpii"], raw_dir / "openpii_train.jsonl", "JSONL"),
        ("nemotron", SOURCE_DATASETS["nemotron"], raw_dir / "nemotron_train.parquet", "PARQUET"),
        ("spy_medical", SOURCE_DATASETS["spy"], raw_dir / "spy_medical.jsonl", "JSONL"),
        ("spy_legal", SOURCE_DATASETS["spy"], raw_dir / "spy_legal.jsonl", "JSONL"),
        ("pii_scope", SOURCE_DATASETS["pii_scope"], raw_dir / "pii_scope" / "pii_scope.csv", "CSV"),
        ("fixture_openpii", SYNTHETIC_DATASETS["openpii"], raw_dir / "fixture_openpii.jsonl", "JSONL"),
        ("fixture_nemotron", SYNTHETIC_DATASETS["nemotron"], raw_dir / "fixture_nemotron.parquet", "PARQUET"),
        ("fixture_spy", SYNTHETIC_DATASETS["spy"], raw_dir / "fixture_spy.jsonl", "JSONL"),
        ("fixture_pii_scope", SYNTHETIC_DATASETS["pii_scope"], raw_dir / "fixture_pii_scope.csv", "CSV"),
    ]
    return [candidate for candidate in candidates if candidate[2].exists()]


def build_batch(
    batch_id: str,
    data_dir: Path,
    limit_per_source: int | None = None,
) -> dict[str, Any]:
    """Build a deterministic staged structured batch and return its manifest."""
    stats = {"malformed": 0}
    grouped: dict[str, list[dict[str, Any]]] = {}
    inputs = _source_inputs(data_dir / "raw")
    real_scope = any(name == "pii_scope" for name, _, _, _ in inputs)
    if not real_scope and not any(name == "fixture_pii_scope" for name, _, _, _ in inputs):
        synthetic_rows = make_pii_scope_fixture(limit_per_source or 8030)
        inputs.append(("synthetic_pii_scope", SYNTHETIC_DATASETS["pii_scope"], Path(), "CSV"))
    else:
        synthetic_rows = iter(())

    all_rows: list[dict[str, Any]] = []
    for source_name, dataset, path, source_format in inputs:
        if source_name == "synthetic_pii_scope":
            iterator: Iterable[dict[str, Any]] = synthetic_rows
        elif source_format == "JSONL":
            iterator = _iter_jsonl_counted(path, stats)
        elif source_format == "PARQUET":
            iterator = _iter_parquet(path)
        else:
            iterator = _iter_csv(path)

        def normalized(
            source_rows: Iterable[dict[str, Any]] = iterator,
            source_dataset: str = dataset,
            row_format: str = source_format,
            input_name: str = source_name,
        ) -> Iterator[dict[str, Any]]:
            for index, row in enumerate(source_rows):
                try:
                    effective_dataset = str(row.get("source_dataset") or source_dataset)
                    if input_name == "synthetic_pii_scope":
                        row = {**row, "id": f"synth-{index:05d}"}
                    yield normalize_source_row(
                        row,
                        batch_id=batch_id,
                        source_dataset=effective_dataset,
                        source_format=row_format,
                        fallback_record_id=f"{input_name}:{index}",
                    )
                except (TypeError, ValueError):
                    stats["malformed"] += 1

        selected = _reservoir(normalized(), limit_per_source)
        grouped[source_name] = selected
        all_rows.extend(selected)

    # Exact duplicate keys can occur when a validation file repeats training rows.
    unique = {row["document_key"]: row for row in all_rows}
    rows = list(unique.values())
    staged_dir = data_dir / "staged" / batch_id
    staged_dir.mkdir(parents=True, exist_ok=True)
    output = staged_dir / "structured.parquet"
    pq.write_table(pa.Table.from_pylist(rows), output, compression="zstd")
    manifest = {
        "batch_id": batch_id,
        "structured_path": str(output),
        "row_count": len(rows),
        "unique_document_keys": len(unique),
        "malformed_rows_skipped": stats["malformed"],
        "sources": {name: len(source_rows) for name, source_rows in grouped.items()},
        "schema_columns": list(STRUCTURED_COLUMNS),
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
    }
    atomic_write_json(staged_dir / "manifest.json", manifest)
    for name, count in manifest["sources"].items():
        print(f"{name}: {count}")
    print(f"total unique document_key count: {len(unique)}; malformed skipped: {stats['malformed']}")
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--limit-per-source", type=int)
    args = parser.parse_args(argv)
    settings = get_settings()
    emit_event("ingestion.build_batches", "START", inputs=["data.raw"], outputs=["data.staged"])
    with traced("ingestion.build_batches", attributes={"batch_id": args.batch_id}):
        manifest = build_batch(args.batch_id, settings.data_dir, args.limit_per_source)
    emit_event(
        "ingestion.build_batches",
        "COMPLETE",
        inputs=["data.raw"],
        outputs=["data.staged"],
        facets={"rows": manifest["row_count"]},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
