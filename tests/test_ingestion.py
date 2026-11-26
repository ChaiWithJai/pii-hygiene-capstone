"""Offline unit and contract tests for the ingestion workstream."""

from __future__ import annotations

import json

import pyarrow.parquet as pq

import ingestion.ocr_extract_worker as ocr_worker
from ingestion import SOURCE_DATASETS, document_key, span_validation_error
from ingestion.build_batches import build_batch, normalize_source_row
from ingestion.fixtures import (
    SYNTHETIC_DATASETS,
    make_offline_source_rows,
    write_offline_source_fixtures,
)
from ingestion.generate_unstructured_docs import generate_documents
from ingestion.inject_bad_records import create_injected_rows, write_injected_batch
from ingestion.ocr_extract_worker import find_candidates


def test_offline_fixture_covers_all_formats_and_isolates_identities(tmp_path) -> None:
    paths = write_offline_source_fixtures(tmp_path / "raw", rows_per_source=2)
    assert {path.suffix for path in paths.values()} == {".jsonl", ".parquet", ".csv"}
    assert len(set(SYNTHETIC_DATASETS.values())) == 4
    assert set(SYNTHETIC_DATASETS.values()).isdisjoint(SOURCE_DATASETS.values())
    assert pq.read_table(paths["nemotron"]).num_rows == 2
    pii_scope_rows = paths["pii_scope"].read_text(encoding="utf-8").splitlines()
    assert pii_scope_rows[1].startswith("synth-pii_scope-")


def test_fixture_spans_are_exact_and_source_names_are_distinct() -> None:
    rows = make_offline_source_rows(2)
    for source, source_rows in rows.items():
        for row in source_rows:
            assert row["source_dataset"] == SYNTHETIC_DATASETS[source]
            assert all(span_validation_error(span, row["text"]) is None for span in row["spans"])


def test_normalize_source_row_obeys_key_and_template_contract() -> None:
    row = {
        "uid": "abc",
        "text": "Email me at fake@example.com",
        "spans": [{"start": 12, "end": 28, "value": "fake@example.com", "label": "EMAIL"}],
        "masked_text": "Email me at {EMAIL}",
    }
    normalized = normalize_source_row(
        row,
        batch_id="b1",
        source_dataset="fixture/example",
        source_format="JSONL",
        fallback_record_id="0",
    )
    assert normalized["document_key"] == document_key("fixture/example", "abc")
    assert normalized["source_format"] == "JSONL"
    assert len(normalized["template_hash"]) == 32


def test_build_batch_reads_jsonl_parquet_and_csv_deterministically(tmp_path) -> None:
    write_offline_source_fixtures(tmp_path / "raw", rows_per_source=4)
    first = build_batch("unit", tmp_path, limit_per_source=2)
    table = pq.read_table(tmp_path / "staged" / "unit" / "structured.parquet")
    assert first["row_count"] == 8
    assert table.column_names == list(first["schema_columns"])
    assert {row["source_format"] for row in table.to_pylist()} == {"JSONL", "PARQUET", "CSV"}
    second = build_batch("unit", tmp_path, limit_per_source=2)
    assert second["sha256"] == first["sha256"]
    manifest = json.loads((tmp_path / "staged" / "unit" / "manifest.json").read_text())
    assert manifest["unique_document_keys"] == 8


def test_bad_record_modes_have_expected_rates_and_files(tmp_path) -> None:
    sample, sample_keys = create_injected_rows("sample", "sample")
    assert len(sample) == 203
    assert len(sample_keys) == 3
    assert sample[-3]["document_key"] == sample[0]["document_key"]
    assert sample[-2]["spans_json"][0]["end"] == 99999
    assert sample[-1]["spans_json"][0]["label"] is None

    flood, flood_keys = create_injected_rows("flood", "flood")
    assert len(flood) == 200
    assert len(flood_keys) == 20
    write_injected_batch("sample", "sample", tmp_path)
    ocr_rows = pq.read_table(tmp_path / "staged" / "sample" / "unstructured.parquet").to_pylist()
    assert ocr_rows[0]["ocr_confidence"] == -0.25


def test_candidate_recognizers_return_aligned_spans() -> None:
    text = "Patient: Maria Gonzalez email maria@example.com SSN 123-45-6789"
    candidates = find_candidates(text)
    labels = {candidate[3] for candidate in candidates}
    assert {"PERSON", "EMAIL", "SSN"} <= labels
    assert all(text[start:end] == value for start, end, value, _ in candidates)


def test_unstructured_generation_has_exact_format_and_perturbation_mix(tmp_path) -> None:
    entries = generate_documents("unit", 10, tmp_path)
    assert sum(entry["file_type"] == "pdf" for entry in entries) == 6
    assert sum(entry["file_type"] == "png" for entry in entries) == 4
    assert sum(entry["perturbed"] for entry in entries) == 5
    assert {entry["doc_type"] for entry in entries} == {
        "clinical_note",
        "invoice",
        "legal_notice",
    }


def test_ocr_batch_contract_without_system_tesseract(tmp_path, monkeypatch) -> None:
    generate_documents("ocr-unit", 1, tmp_path)
    text = "Email fake@example.com"
    words = [
        {"start": 0, "end": 5, "bbox": [1, 2, 20, 12], "confidence": 0.99},
        {"start": 6, "end": 22, "bbox": [22, 2, 100, 12], "confidence": 0.91},
    ]
    monkeypatch.setattr(ocr_worker, "_images", lambda _path: [object()])
    monkeypatch.setattr(ocr_worker, "_ocr_page", lambda _image: (text, words))
    rows = ocr_worker.extract_batch("ocr-unit", tmp_path)
    assert len(rows) == 1
    assert rows[0]["source_format"] == "PDF/PNG"
    assert rows[0]["source_file_uri"].startswith("local://unstructured/ocr-unit/")
    assert rows[0]["bbox_json"] == [22, 2, 100, 12]
    assert rows[0]["ocr_confidence"] == 0.91
