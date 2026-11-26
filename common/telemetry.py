"""OpenTelemetry trace propagation with a zero-dependency local fallback.

Every pipeline stage emits a span-shaped JSON line plus a stable 32-hex trace id.
When OTEL_EXPORTER_OTLP_ENDPOINT is set, spans are also exported over OTLP/HTTP
to the local otel-collector (or Cloud Trace / any OTLP backend in GCP).
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any

from common.config import get_settings


def new_trace_id() -> str:
    return uuid.uuid4().hex


def _write_event(event: dict[str, Any]) -> None:
    settings = get_settings()
    directory = settings.observability_dir
    try:
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "spans.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, default=str) + "\n")
    except OSError:
        # Observability must never take down a data pipeline.
        return


@contextmanager
def traced(
    stage: str,
    trace_id: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> Iterator[str]:
    """Context manager yielding the trace id used for this stage."""
    trace_id = trace_id or os.environ.get("PII_TRACE_ID") or new_trace_id()
    started = datetime.now(UTC)
    exc: BaseException | None = None
    try:
        yield trace_id
    except BaseException as error:  # noqa: BLE001 - re-raised after logging
        exc = error
        raise
    finally:
        ended = datetime.now(UTC)
        event = {
            "trace_id": trace_id,
            "span_id": uuid.uuid4().hex[:16],
            "name": stage,
            "started_at": started.isoformat(),
            "ended_at": ended.isoformat(),
            "duration_ms": round((ended - started).total_seconds() * 1000, 2),
            "status": "ERROR" if exc else "OK",
            "error": type(exc).__name__ if exc else None,
            "attributes": attributes or {},
        }
        _write_event(event)
        _export_otlp(event)


def _export_otlp(event: dict[str, Any]) -> None:
    endpoint = get_settings().otel_endpoint
    if not endpoint:
        return
    try:  # pragma: no cover - network path, optional by design
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

        provider = trace.get_tracer_provider()
        if not isinstance(provider, TracerProvider):
            provider = TracerProvider(resource=Resource.create({"service.name": "pii-hygiene"}))
            provider.add_span_processor(
                BatchSpanProcessor(OTLPSpanExporter(endpoint=f"{endpoint}/v1/traces"))
            )
            trace.set_tracer_provider(provider)
        tracer = provider.get_tracer("pii-hygiene")
        parent = SpanContext(
            trace_id=int(event["trace_id"], 16),
            span_id=int(event["trace_id"][:16], 16) or 1,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        with tracer.start_as_current_span(
            event["name"],
            context=trace.set_span_in_context(NonRecordingSpan(parent)),
            start_time=int(datetime.fromisoformat(event["started_at"]).timestamp() * 1e9),
            end_on_exit=False,
        ) as span:
            span.set_attribute("pii.trace_id", event["trace_id"])
            span.set_attribute("pii.duration_ms", event["duration_ms"])
            for key, value in event["attributes"].items():
                span.set_attribute(f"pii.{key}", str(value))
            if event["error"]:
                span.set_status(trace.Status(trace.StatusCode.ERROR, event["error"]))
            span.end(end_time=int(datetime.fromisoformat(event["ended_at"]).timestamp() * 1e9))
        provider.force_flush(timeout_millis=2000)
    except Exception:  # pragma: no cover - observability must never break the run
        return
