"""OCR generated documents and stage one row per candidate PII span."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from statistics import mean
from typing import Any

import fitz
import pyarrow as pa
import pyarrow.parquet as pq
import pytesseract
from PIL import Image
from pytesseract import Output

from common.config import get_settings
from common.lineage import emit_event
from common.telemetry import traced
from ingestion import UNSTRUCTURED_COLUMNS

SOURCE_DATASET = "synthetic-generated/unstructured-documents"

PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("EMAIL", re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    ("PHONE", re.compile(r"(?:\+?1[-. ]?)?\(?\d{3}\)?[-. ]\d{3}[-.]\d{4}")),
    ("SSN", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("CREDIT_CARD", re.compile(r"\b(?:\d{4}[ -]){3}\d{4}\b")),
    ("MRN", re.compile(r"\bMRN[- :]+[A-Z0-9-]{4,}\b", re.I)),
    ("ACCOUNT_NUMBER", re.compile(r"\b(?:ACC|account(?: number)?)[- #:]+[A-Z0-9-]{4,}\b", re.I)),
    ("DATE", re.compile(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b|\b[A-Z][a-z]+ \d{1,2}, (?:19|20)\d{2}\b")),
    ("PERSON", re.compile(r"(?:(?:Patient|Member|Client|Plaintiff|To|From|holder|Witness):?\s+)([A-Z][a-z]+(?:\s+[A-Z][A-Za-z']+)+)")),
)


def find_candidates(text: str) -> list[tuple[int, int, str, str]]:
    """Return deterministic non-duplicate candidate spans in page text."""
    candidates: list[tuple[int, int, str, str]] = []
    seen: set[tuple[int, int, str]] = set()
    for label, pattern in PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.span(1) if label == "PERSON" and match.lastindex else match.span()
            key = (start, end, label)
            if key not in seen:
                candidates.append((start, end, text[start:end], label))
                seen.add(key)
    return sorted(candidates)


def _ocr_page(image: Image.Image) -> tuple[str, list[dict[str, Any]]]:
    data = pytesseract.image_to_data(image, config="--psm 6", output_type=Output.DICT)
    words: list[dict[str, Any]] = []
    chunks: list[str] = []
    cursor = 0
    for index, raw_word in enumerate(data["text"]):
        word = str(raw_word).strip()
        if not word:
            continue
        if chunks:
            chunks.append(" ")
            cursor += 1
        start = cursor
        chunks.append(word)
        cursor += len(word)
        try:
            confidence = max(0.0, min(1.0, float(data["conf"][index]) / 100.0))
        except (TypeError, ValueError):
            confidence = 0.0
        words.append(
            {
                "start": start,
                "end": cursor,
                "bbox": [
                    int(data["left"][index]),
                    int(data["top"][index]),
                    int(data["left"][index]) + int(data["width"][index]),
                    int(data["top"][index]) + int(data["height"][index]),
                ],
                "confidence": confidence,
            }
        )
    return "".join(chunks), words


def _bbox_and_confidence(
    words: list[dict[str, Any]], start: int, end: int
) -> tuple[list[int], float]:
    selected = [word for word in words if word["end"] > start and word["start"] < end]
    if not selected:
        return [0, 0, 0, 0], 0.0
    boxes = [word["bbox"] for word in selected]
    bbox = [
        min(box[0] for box in boxes),
        min(box[1] for box in boxes),
        max(box[2] for box in boxes),
        max(box[3] for box in boxes),
    ]
    return bbox, mean(word["confidence"] for word in selected)


def _images(path: Path) -> list[Image.Image]:
    if path.suffix.lower() == ".png":
        with Image.open(path) as image:
            return [image.convert("RGB")]
    document = fitz.open(path)
    images: list[Image.Image] = []
    try:
        matrix = fitz.Matrix(200 / 72, 200 / 72)
        for page in document:
            pixmap = page.get_pixmap(matrix=matrix, alpha=False)
            images.append(Image.frombytes("RGB", (pixmap.width, pixmap.height), pixmap.samples))
    finally:
        document.close()
    return images


def extract_batch(batch_id: str, data_dir: Path, limit: int | None = None) -> list[dict[str, Any]]:
    """OCR a generated batch and write the unstructured staging Parquet."""
    source_dir = data_dir / "unstructured" / batch_id
    manifest = json.loads((source_dir / "manifest.json").read_text(encoding="utf-8"))
    documents = manifest.get("documents", [])
    if limit is not None:
        documents = documents[:limit]
    rows: list[dict[str, Any]] = []
    for entry in documents:
        path = source_dir / entry["filename"]
        relative_path = entry.get("relative_file_path", entry["filename"])
        doc_key = hashlib.sha256(f"unstructured|{batch_id}|{relative_path}".encode()).hexdigest()
        span_index = 0
        for page_number, image in enumerate(_images(path), start=1):
            text, words = _ocr_page(image)
            for start, end, value, label in find_candidates(text):
                bbox, confidence = _bbox_and_confidence(words, start, end)
                extraction_key = hashlib.sha256(
                    f"{doc_key}|{page_number}|{span_index}".encode()
                ).hexdigest()
                rows.append(
                    {
                        "batch_id": batch_id,
                        "extraction_key": extraction_key,
                        "document_key": doc_key,
                        "source_dataset": SOURCE_DATASET,
                        "source_format": "PDF/PNG",
                        "source_file_uri": f"local://unstructured/{batch_id}/{entry['filename']}",
                        "file_type": entry["file_type"],
                        "page_number": page_number,
                        "bbox_json": bbox,
                        "ocr_confidence": confidence,
                        "extracted_value": value,
                        "raw_entity_label": label,
                        "extracted_text": text,
                        "language": entry.get("language", "en"),
                        "domain": entry.get("domain", "general"),
                        "template_hash": entry.get("template_hash"),
                        "is_ocr_perturbed": bool(entry.get("perturbed", False)),
                    }
                )
                span_index += 1
    staged_dir = data_dir / "staged" / batch_id
    staged_dir.mkdir(parents=True, exist_ok=True)
    if rows:
        table = pa.Table.from_pylist(rows)
    else:
        table = pa.table({column: pa.array([], type=pa.string()) for column in UNSTRUCTURED_COLUMNS})
    pq.write_table(table, staged_dir / "unstructured.parquet", compression="zstd")
    confidence = mean(row["ocr_confidence"] for row in rows) if rows else 0.0
    print(f"files processed: {len(documents)}; rows written: {len(rows)}; mean confidence: {confidence:.3f}")
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--limit", type=int)
    args = parser.parse_args(argv)
    settings = get_settings()
    emit_event("ingestion.ocr_extract_worker", "START", inputs=["data.unstructured"], outputs=["data.staged"])
    with traced("ingestion.ocr_extract_worker", attributes={"batch_id": args.batch_id}):
        rows = extract_batch(args.batch_id, settings.data_dir, args.limit)
    emit_event(
        "ingestion.ocr_extract_worker",
        "COMPLETE",
        inputs=["data.unstructured"],
        outputs=["data.staged"],
        facets={"rows": len(rows)},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
