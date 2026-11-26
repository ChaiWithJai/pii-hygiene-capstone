"""Ingestion package for the local PII hygiene platform.

This package owns source acquisition, normalization into staged Parquet files,
synthetic unstructured document generation, OCR extraction and loading into the
Postgres ``raw`` schema.  The public helpers in this module are deliberately
pure and dependency-light so they can be reused by the CLIs and unit tests.

Contracts implemented here follow ``docs/internal/SPEC.md``:

* ``document_key = sha256(f"{source_dataset}|{record_id}").hexdigest()``
* ``template_hash = sha256(masked/templated text).hexdigest()[:32]``
* spans are ``{"start": int, "end": int, "value": str, "label": str}`` with
  zero-based, end-exclusive Python string offsets.  ``label`` is always the
  original source label; normalization never rewrites it to a canonical label.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

__all__ = [
    "SOURCE_DATASETS",
    "STRUCTURED_COLUMNS",
    "UNSTRUCTURED_COLUMNS",
    "atomic_write_json",
    "atomic_write_text",
    "compute_template_hash",
    "document_key",
    "iter_jsonl",
    "make_span",
    "mask_text_with_spans",
    "normalize_span",
    "normalize_spans",
    "now_utc_iso",
    "parse_span_payload",
    "span_validation_error",
    "utc_timestamp_compact",
]

# Exact ``source_dataset`` values required by the shared contract.
SOURCE_DATASETS: dict[str, str] = {
    "openpii": "ai4privacy/pii-masking-openpii-1.5m",
    "nemotron": "nvidia/Nemotron-PII",
    "pii_scope": "Protegrity/pii-scope",
    "spy": "mks-logic/SPY",
}

# ``raw.pii_documents_raw`` minus the database-managed ``ingested_at`` column.
STRUCTURED_COLUMNS: tuple[str, ...] = (
    "batch_id",
    "document_key",
    "source_dataset",
    "source_format",
    "record_id",
    "raw_text",
    "spans_json",
    "language",
    "domain",
    "template_hash",
)

# ``raw.unstructured_extractions_raw`` minus ``ingested_at``.
UNSTRUCTURED_COLUMNS: tuple[str, ...] = (
    "batch_id",
    "extraction_key",
    "document_key",
    "source_dataset",
    "source_format",
    "source_file_uri",
    "file_type",
    "page_number",
    "bbox_json",
    "ocr_confidence",
    "extracted_value",
    "raw_entity_label",
    "extracted_text",
    "language",
    "domain",
    "template_hash",
    "is_ocr_perturbed",
)


def now_utc_iso() -> str:
    """Return the current UTC timestamp in ISO-8601 form."""
    return datetime.now(UTC).isoformat()


def utc_timestamp_compact() -> str:
    """Return a compact UTC timestamp such as ``20261126T061500Z``."""
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def document_key(source_dataset: str, record_id: str) -> str:
    """Compute the contract document key for a source row."""
    return hashlib.sha256(f"{source_dataset}|{record_id}".encode()).hexdigest()


def compute_template_hash(templated_text: str) -> str:
    """Compute the 32-character group-aware split key for a document."""
    return hashlib.sha256(templated_text.encode("utf-8")).hexdigest()[:32]


def make_span(start: int, end: int, value: str, label: str | None) -> dict[str, Any]:
    """Build a normalized span while retaining the original raw label."""
    return {"start": int(start), "end": int(end), "value": value, "label": label}


def normalize_span(raw: Mapping[str, Any], text: str | None = None) -> dict[str, Any]:
    """Normalize a source-specific span dict.

    Supported aliases across corpora: ``start``/``start_offset`` and
    ``end``/``end_offset``; ``value``/``text``/``span_value``.  When a value is
    absent and ``text`` is supplied it is sliced from the original document.
    The label is copied verbatim (no taxonomy mapping) for auditability.
    """
    start_raw = raw.get("start", raw.get("start_offset"))
    end_raw = raw.get("end", raw.get("end_offset"))
    if start_raw is None or end_raw is None:
        raise ValueError("span is missing start/end offsets")

    start = int(start_raw)
    end = int(end_raw)
    value_raw = raw.get("value", raw.get("text", raw.get("span_value")))
    if value_raw is None and text is not None and 0 <= start <= end <= len(text):
        value = text[start:end]
    else:
        value = "" if value_raw is None else str(value_raw)
    label_raw = raw.get("label", raw.get("raw_label", raw.get("entity_label")))
    label = None if label_raw is None else str(label_raw)
    return make_span(start, end, value, label)


def normalize_spans(
    raw_spans: Any,
    text: str | None = None,
) -> list[dict[str, Any]]:
    """Normalize an iterable of source spans, skipping structurally bad entries.

    A span with a missing offset is dropped.  Offsets that are merely out of
    bounds are preserved: downstream dbt quality models must quarantine those
    rows rather than have ingestion silently repair or hide them.
    """
    if raw_spans is None:
        return []
    if isinstance(raw_spans, str):
        raw_spans = parse_span_payload(raw_spans)
    if not isinstance(raw_spans, Iterable):
        return []

    normalized: list[dict[str, Any]] = []
    for raw in raw_spans:
        if not isinstance(raw, Mapping):
            continue
        try:
            normalized.append(normalize_span(raw, text))
        except (TypeError, ValueError):
            continue
    return normalized


def parse_span_payload(payload: Any) -> list[dict[str, Any]]:
    """Parse JSON or Python-literal span payloads without raising.

    OpenPII uses a native JSON list, Nemotron stores a Python ``repr`` string
    and the synthetic PII-SCOPE CSV stores a JSON string.
    """
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    if not isinstance(payload, str) or not payload.strip():
        return []
    import ast

    try:
        parsed = json.loads(payload)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(payload)
        except (SyntaxError, ValueError):
            return []
    if isinstance(parsed, Mapping):
        parsed = [parsed]
    if not isinstance(parsed, list):
        return []
    return [dict(item) for item in parsed if isinstance(item, Mapping)]


def span_validation_error(span: Mapping[str, Any], text: str) -> str | None:
    """Validate one span against the raw text.

    Returns ``None`` when valid, otherwise one of the contract failure reasons
    ``OUT_OF_BOUNDS_OFFSET`` or ``SUBSTRING_MISMATCH``.  ``UNMAPPED_ENTITY_LABEL``
    is handled by the dbt taxonomy layer, not by this structural helper.
    """
    start = int(span.get("start", -1))
    end = int(span.get("end", -1))
    if start < 0 or end < 0 or start > end or end > len(text):
        return "OUT_OF_BOUNDS_OFFSET"
    if str(span.get("value", "")) != text[start:end]:
        return "SUBSTRING_MISMATCH"
    return None


def mask_text_with_spans(text: str, spans: Sequence[Mapping[str, Any]]) -> str:
    """Replace span values with ``[LABEL]`` placeholders from right to left.

    The result is the templated form used to derive ``template_hash``.  Invalid
    or overlapping spans are skipped so masking always terminates.
    """
    masked = text
    last_start: int | None = None
    ordered = sorted(
        (s for s in spans if _valid_offsets_for_mask(s, text)),
        key=lambda s: int(s["start"]),
        reverse=True,
    )
    for span in ordered:
        start = int(span["start"])
        end = int(span["end"])
        if last_start is not None and end > last_start:
            continue
        label = span.get("label")
        placeholder = f"[{label}]" if label is not None else "[PII]"
        masked = masked[:start] + placeholder + masked[end:]
        last_start = start
    return masked


def _valid_offsets_for_mask(span: Mapping[str, Any], text: str) -> bool:
    try:
        start = int(span["start"])
        end = int(span["end"])
    except (KeyError, TypeError, ValueError):
        return False
    if start < 0 or end < start or end > len(text):
        return False
    return str(span.get("value", "")) == text[start:end]


def san(value: Any) -> Any:
    """Convert non-JSON scalar types to JSON-safe values.

    Kept tiny and local; only used for manifest serialization.
    """
    if value is None or isinstance(value, bool | int | float | str):
        return value
    return str(value)


def atomic_write_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    """Write text via a sibling temporary file then atomically rename."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        tmp.write_text(text, encoding=encoding)
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def atomic_write_json(path: Path, payload: Any) -> None:
    """Serialize ``payload`` as pretty JSON with an atomic replace."""
    atomic_write_text(path, json.dumps(payload, indent=2, default=san) + "\n")


def iter_jsonl(path: Path) -> Iterator[dict[str, Any]]:
    """Yield JSON objects from a JSONL file, skipping malformed lines.

    Callers that need skip counts should count themselves; this helper is aimed
    at simple fixtures and tests.
    """
    with path.open("r", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, Mapping):
                yield dict(value)
