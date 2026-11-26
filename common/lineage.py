"""Minimal OpenLineage 2.0 emitter.

Emits dataset-level lineage JSON for each pipeline stage. Events are written to
data/observability/lineage.jsonl and optionally POSTed to OPENLINEAGE_URL
(Marquez locally, Dataplex Lineage on GCP).
"""

from __future__ import annotations

import json
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from typing import Any

import requests

from common.config import get_settings

PRODUCER = "https://github.com/pii-hygiene-capstone/pii-hygiene-capstone"


def _now() -> str:
    return datetime.now(UTC).isoformat()


def emit_event(
    job_name: str,
    event_type: str,
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    run_id: str | None = None,
    facets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = get_settings()
    namespace = "pii_lab.local"
    event = {
        "eventType": event_type,
        "eventTime": _now(),
        "producer": PRODUCER,
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        "run": {
            "runId": run_id or str(uuid.uuid4()),
            "facets": {"pii_pipeline": {"batch_id": settings.batch_id, **(facets or {})}},
        },
        "job": {"namespace": namespace, "name": job_name},
        "inputs": [
            {"namespace": namespace, "name": name} for name in (inputs or [])
        ],
        "outputs": [
            {"namespace": namespace, "name": name} for name in (outputs or [])
        ],
    }
    directory = settings.observability_dir
    directory.mkdir(parents=True, exist_ok=True)
    try:
        with (directory / "lineage.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event) + "\n")
    except OSError:
        pass
    if settings.openlineage_url:
        with suppress(requests.RequestException):  # pragma: no cover - optional network path
            requests.post(
                f"{settings.openlineage_url.rstrip('/')}/api/v1/lineage",
                json=event,
                timeout=3,
            )
    return event
