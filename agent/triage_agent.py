"""Quality-gated PII triage agent with a deterministic, auditable policy.

The policy is a pure function (:func:`decide`) so it can be unit-tested without
a database. All state changes are bounded by an allowlist of action tools, a
Pydantic v2 payload schema, and a SHA256 idempotency key. No external network
call happens unless ``SANDBOX_GITHUB_REPO`` and ``SANDBOX_GITHUB_TOKEN`` are
explicitly configured.

Usage::

    python -m agent.triage_agent [--max-actions N]
    python -m agent.triage_agent --self-test
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# ---------------------------------------------------------------------------
# Allowlisted policy (exactly four decisions, three action tools)
# ---------------------------------------------------------------------------

DECISION_OCR = "OPEN_OCR_VERIFICATION_TASK"
DECISION_REDACTION = "OPEN_REDACTION_REVIEW_TASK"
DECISION_SLICE = "QUARANTINE_SLICE_ALERT"
DECISION_HOLD = "HOLD_AND_LOG"
DECISION_BLOCKED = "BLOCKED_DQ_FAIL"

ACTION_INSERT_REVIEW_QUEUE = "INSERT_REVIEW_QUEUE"
ACTION_INSERT_SLICE_ALERT = "INSERT_SLICE_ALERT"
ACTION_NO_STATE_CHANGE = "NO_STATE_CHANGE"

TASK_TYPE_OCR = "OCR_VERIFICATION"
TASK_TYPE_REDACTION = "REDACTION_REVIEW"
TASK_TYPE_SLICE = "SLICE_QUARANTINE_ALERT"

ALLOWED_DECISIONS: frozenset[str] = frozenset(
    {DECISION_OCR, DECISION_REDACTION, DECISION_SLICE, DECISION_HOLD}
)
ALLOWED_ACTION_TOOLS: frozenset[str] = frozenset(
    {ACTION_INSERT_REVIEW_QUEUE, ACTION_INSERT_SLICE_ALERT, ACTION_NO_STATE_CHANGE}
)
ALLOWED_TASK_TYPES: frozenset[str] = frozenset(
    {TASK_TYPE_OCR, TASK_TYPE_REDACTION, TASK_TYPE_SLICE}
)
ALLOWED_PRIORITIES: frozenset[str] = frozenset({"HIGH", "MEDIUM", "LOW"})

POLICY_MATRIX: dict[str, tuple[str, str | None, str]] = {
    "OCR_VERIFICATION": (
        DECISION_OCR,
        TASK_TYPE_OCR,
        ACTION_INSERT_REVIEW_QUEUE,
    ),
    "REDACTION_REVIEW": (
        DECISION_REDACTION,
        TASK_TYPE_REDACTION,
        ACTION_INSERT_REVIEW_QUEUE,
    ),
    "SLICE_QUARANTINE_ALERT": (
        DECISION_SLICE,
        TASK_TYPE_SLICE,
        ACTION_INSERT_SLICE_ALERT,
    ),
    "VERIFIED_FALSE_POSITIVE": (DECISION_HOLD, None, ACTION_NO_STATE_CHANGE),
}

AGENT_NAME = "pii_triage_agent"
AGENT_VERSION = "1.0.0"
TRACE_JOB_NAME = "agent_triage_agent"
BLOCKED_DOCUMENT_KEY = "__pipeline__"
SANDBOX_LABEL = "pii-sandbox"
_GITHUB_REPO_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Decision:
    """Result of applying the deterministic policy to one candidate."""

    decision: str
    task_type: str | None
    action_tool: str
    priority: str
    reason: str


def _normalise_priority(value: Any, default: str = "MEDIUM") -> str:
    priority = str(value or "").strip().upper()
    return priority if priority in ALLOWED_PRIORITIES else default


def _derive_priority(candidate_type: str, candidate: Mapping[str, Any]) -> str:
    explicit = str(candidate.get("priority") or "").strip().upper()
    if explicit in ALLOWED_PRIORITIES:
        return explicit
    if candidate_type == "SLICE_QUARANTINE_ALERT":
        return "HIGH"
    if candidate_type == "VERIFIED_FALSE_POSITIVE":
        return "LOW"
    if candidate_type == "OCR_VERIFICATION":
        confidence = _as_float(candidate.get("ocr_confidence"))
        return "HIGH" if confidence is not None and confidence < 0.8 else "MEDIUM"
    if candidate_type == "REDACTION_REVIEW":
        density = _as_float(candidate.get("entity_density"))
        overlap = candidate.get("has_overlapping_spans")
        return "HIGH" if overlap is True or (density or 0.0) > 2.5 else "MEDIUM"
    return "MEDIUM"


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def decide(candidate: Mapping[str, Any]) -> Decision:
    """Apply the allowlisted policy to a single triage candidate.

    Unknown candidate types and malformed rows safely fall through to
    ``HOLD_AND_LOG``; the agent never invents an action.
    """
    candidate_type = str(candidate.get("candidate_type") or "").strip().upper()
    mapping = POLICY_MATRIX.get(candidate_type)
    if mapping is None:
        return Decision(
            decision=DECISION_HOLD,
            task_type=None,
            action_tool=ACTION_NO_STATE_CHANGE,
            priority="LOW",
            reason=f"unknown candidate_type={candidate_type or '<missing>'}; "
            "safe fallback HOLD_AND_LOG",
        )
    decision, task_type, action_tool = mapping
    if action_tool not in ALLOWED_ACTION_TOOLS:
        return Decision(
            decision=DECISION_HOLD,
            task_type=None,
            action_tool=ACTION_NO_STATE_CHANGE,
            priority="LOW",
            reason=f"action {action_tool!r} is not allowlisted; HOLD_AND_LOG",
        )
    return Decision(
        decision=decision,
        task_type=task_type,
        action_tool=action_tool,
        priority=_derive_priority(candidate_type, candidate),
        reason=f"candidate_type={candidate_type} -> {decision}",
    )


def idempotency_key(
    document_key: str,
    task_type_or_decision: str,
    batch_id: str,
) -> str:
    """Stable SHA256 key used for both task_id and audit deduplication."""
    raw = f"{document_key}|{task_type_or_decision}|{batch_id}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def jsonable(value: Any) -> Any:
    """Recursively convert DB values into JSON-serialisable primitives."""
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple | set):
        return [jsonable(item) for item in value]
    return str(value)


def build_evidence(candidate: Mapping[str, Any], reason: str) -> dict[str, Any]:
    """Build a JSON-serialisable evidence payload for queue + audit rows."""
    evidence = {
        "candidate": jsonable(dict(candidate)),
        "policy_reason": reason,
        "agent": AGENT_NAME,
        "agent_version": AGENT_VERSION,
    }
    # Never let a stray non-JSON value reach the warehouse.
    return json.loads(json.dumps(evidence, default=str))


# ---------------------------------------------------------------------------
# Pydantic v2 validation
# ---------------------------------------------------------------------------


class TaskPayload(BaseModel):
    """Validated payload for an insert into ``governance.agent_review_queue``."""

    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(min_length=1, max_length=128)
    task_type: Literal[
        "REDACTION_REVIEW", "OCR_VERIFICATION", "SLICE_QUARANTINE_ALERT"
    ]
    document_key: str = Field(min_length=1, max_length=512)
    batch_id: str = Field(min_length=1, max_length=256)
    priority: Literal["HIGH", "MEDIUM", "LOW"] = "MEDIUM"
    evidence_json: dict[str, Any]
    source_file_uri: str | None = None

    @field_validator("evidence_json")
    @classmethod
    def _evidence_must_serialise(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            json.dumps(value, default=str)
        except (TypeError, ValueError) as error:  # pragma: no cover - defensive
            raise ValueError(
                f"evidence_json is not JSON serialisable: {error}"
            ) from error
        return value


class AuditRow(BaseModel):
    """Validated record for the immutable ``governance.agent_action_log``."""

    model_config = ConfigDict(extra="forbid")

    action_id: str = Field(min_length=1, max_length=128)
    task_id: str | None = None
    document_key: str = Field(min_length=1, max_length=512)
    source_file_uri: str | None = None
    batch_id: str = Field(min_length=1, max_length=256)
    evidence_json: dict[str, Any]
    decision: str
    action_taken: str
    outcome: str
    otel_trace_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("evidence_json")
    @classmethod
    def _evidence_must_serialise(cls, value: dict[str, Any]) -> dict[str, Any]:
        try:
            json.dumps(value, default=str)
        except (TypeError, ValueError) as error:  # pragma: no cover - defensive
            raise ValueError(
                f"evidence_json is not JSON serialisable: {error}"
            ) from error
        return value


def build_task_payload(
    candidate: Mapping[str, Any],
    decision: Decision,
) -> TaskPayload:
    """Validate and construct a queue task payload from a candidate."""
    if decision.task_type is None:
        raise ValueError("HOLD_AND_LOG decisions do not create queue tasks")
    document_key = str(candidate.get("document_key") or "")
    batch_id = str(candidate.get("batch_id") or "")
    key = idempotency_key(document_key, decision.task_type, batch_id)
    evidence = build_evidence(candidate, decision.reason)
    return TaskPayload(
        task_id=key,
        task_type=decision.task_type,
        document_key=document_key,
        batch_id=batch_id,
        priority=_normalise_priority(decision.priority),
        evidence_json=evidence,
        source_file_uri=(
            str(candidate["source_file_uri"])
            if candidate.get("source_file_uri")
            else None
        ),
    )


def build_audit_row(
    *,
    document_key: str,
    batch_id: str,
    source_file_uri: str | None,
    evidence_json: dict[str, Any],
    decision: str,
    action_taken: str,
    outcome: str,
    otel_trace_id: str,
    task_type_or_decision: str,
    task_id: str | None = None,
) -> AuditRow:
    """Validate and construct one immutable audit row."""
    key = idempotency_key(document_key, task_type_or_decision, batch_id)
    return AuditRow(
        action_id=key,
        task_id=task_id,
        document_key=document_key or "unknown",
        source_file_uri=source_file_uri,
        batch_id=batch_id or "unknown",
        evidence_json=evidence_json,
        decision=decision,
        action_taken=action_taken,
        outcome=outcome,
        otel_trace_id=otel_trace_id,
        idempotency_key=key,
    )


# ---------------------------------------------------------------------------
# Database orchestration (import-time safe)
# ---------------------------------------------------------------------------


def _latest_dbt_test_status(conn: Any, batch_id: str) -> str | None:
    """Return the latest dbt test status for ``batch_id``, or ``None``.

    A PASS from another batch must never authorize actions for the active
    batch.  The pipeline can retain metadata for many batches, so the gate is
    deliberately scoped before applying the recency ordering.
    """
    from common.db import table_exists

    if not table_exists(conn, "governance", "dbt_run_metadata"):
        return None
    with conn.cursor() as cur:
        cur.execute(
            "SELECT test_status FROM governance.dbt_run_metadata "
            "WHERE batch_id = %s "
            "ORDER BY created_at DESC, dbt_run_started_at DESC LIMIT 1",
            (batch_id,),
        )
        row = cur.fetchone()
    return str(row[0]) if row else None


def _insert_blocked_row(
    conn: Any,
    *,
    batch_id: str,
    status: str | None,
    trace_id: str,
) -> bool:
    """Insert exactly one idempotent BLOCKED_DQ_FAIL audit row."""
    from psycopg2.extras import Json

    reason = (
        "latest governance.dbt_run_metadata.test_status is not PASS "
        f"(status={status or 'MISSING'})"
    )
    evidence = json.loads(
        json.dumps(
            {
                "decision": DECISION_BLOCKED,
                "reason": reason,
                "dbt_test_status": status or "MISSING",
                "agent": AGENT_NAME,
                "agent_version": AGENT_VERSION,
            }
        )
    )
    row = build_audit_row(
        document_key=BLOCKED_DOCUMENT_KEY,
        batch_id=batch_id,
        source_file_uri=None,
        evidence_json=evidence,
        decision=DECISION_BLOCKED,
        action_taken=ACTION_NO_STATE_CHANGE,
        outcome=DECISION_BLOCKED,
        otel_trace_id=trace_id,
        task_type_or_decision=DECISION_BLOCKED,
    )
    with conn:  # noqa: SIM117 - transaction must wrap the cursor lifecycle
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO governance.agent_action_log (
                    action_id, task_id, document_key, source_file_uri,
                    batch_id, evidence_json, decision, action_taken,
                    outcome, otel_trace_id, idempotency_key
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                """,
                (
                    row.action_id,
                    row.task_id,
                    row.document_key,
                    row.source_file_uri,
                    row.batch_id,
                    Json(row.evidence_json),
                    row.decision,
                    row.action_taken,
                    row.outcome,
                    row.otel_trace_id,
                    row.idempotency_key,
                ),
            )
            inserted = cur.rowcount > 0
    return inserted


def _insert_queue_task(conn: Any, payload: TaskPayload) -> bool:
    """Insert a review-queue task; ``False`` means it already existed."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO governance.agent_review_queue (
                task_id, task_type, document_key, source_file_uri, batch_id,
                priority, status, assigned_role, evidence_json, created_by
            ) VALUES (%s, %s, %s, %s, %s, %s, 'OPEN', %s, %s, %s)
            ON CONFLICT (task_id) DO NOTHING
            """,
            (
                payload.task_id,
                payload.task_type,
                payload.document_key,
                payload.source_file_uri,
                payload.batch_id,
                payload.priority,
                "PRIVACY_REVIEWER",
                Json(payload.evidence_json),
                AGENT_NAME,
            ),
        )
        return cur.rowcount > 0


def _insert_audit_row(conn: Any, row: AuditRow) -> bool:
    """Insert an audit row; ``False`` means the idempotency key exists."""
    from psycopg2.extras import Json

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO governance.agent_action_log (
                action_id, task_id, document_key, source_file_uri, batch_id,
                evidence_json, decision, action_taken, outcome, otel_trace_id,
                idempotency_key
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idempotency_key) DO NOTHING
            """,
            (
                row.action_id,
                row.task_id,
                row.document_key,
                row.source_file_uri,
                row.batch_id,
                Json(row.evidence_json),
                row.decision,
                row.action_taken,
                row.outcome,
                row.otel_trace_id,
                row.idempotency_key,
            ),
        )
        return cur.rowcount > 0


def _sandbox_issue(
    *,
    repo: str,
    token: str,
    payload: TaskPayload,
    timeout: float = 5.0,
) -> tuple[bool, str | None, str | None]:
    """Optionally open a sandbox GitHub issue. Never raises."""
    if not repo or not token:
        return False, None, None
    if not _GITHUB_REPO_RE.match(repo):
        return False, None, f"repo {repo!r} is not an allowlisted owner/name"
    try:
        import requests

        response = requests.post(
            f"https://api.github.com/repos/{repo}/issues",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            json={
                "title": (
                    f"[pii-sandbox] {payload.task_type} "
                    f"{payload.document_key[:12]}"
                ),
                "body": (
                    "Automated PII triage sandbox issue.\n\n```json\n"
                    + json.dumps(payload.evidence_json, indent=2, default=str)
                    + "\n```"
                ),
                "labels": [SANDBOX_LABEL],
            },
            timeout=timeout,
        )
        if response.status_code >= 400:
            return False, None, f"HTTP {response.status_code}: {response.text[:200]}"
        return True, response.json().get("html_url"), None
    except Exception as error:  # noqa: BLE001 - external call must not break run
        return False, None, f"{type(error).__name__}: {error}"


def _process_candidate(
    conn: Any,
    candidate: Mapping[str, Any],
    *,
    trace_id: str,
    github_repo: str,
    github_token: str,
) -> tuple[str, str, bool, bool, str | None]:
    """Apply policy to one candidate and persist the outcome.

    Returns ``(decision, outcome, action_inserted, audit_inserted, error)``.
    """
    decision = decide(candidate)
    document_key = str(candidate.get("document_key") or "")
    batch_id = str(candidate.get("batch_id") or "")
    source_file_uri = candidate.get("source_file_uri")
    source_file_uri = str(source_file_uri) if source_file_uri else None

    if document_key == "" or batch_id == "":
        decision = Decision(
            decision=DECISION_HOLD,
            task_type=None,
            action_tool=ACTION_NO_STATE_CHANGE,
            priority="LOW",
            reason="candidate is missing document_key or batch_id; HOLD_AND_LOG",
        )

    if decision.decision == DECISION_HOLD:
        evidence = build_evidence(candidate, decision.reason)
        row = build_audit_row(
            document_key=document_key,
            batch_id=batch_id,
            source_file_uri=source_file_uri,
            evidence_json=evidence,
            decision=decision.decision,
            action_taken=decision.action_tool,
            outcome="SUCCESS",
            otel_trace_id=trace_id,
            task_type_or_decision=decision.decision,
        )
        with conn:
            audit_inserted = _insert_audit_row(conn, row)
        return decision.decision, row.outcome, False, audit_inserted, None

    try:
        payload = build_task_payload(candidate, decision)
    except (ValidationError, ValueError) as error:
        evidence = build_evidence(
            candidate,
            f"payload validation failed: {type(error).__name__}: {error}",
        )
        row = build_audit_row(
            document_key=document_key,
            batch_id=batch_id,
            source_file_uri=source_file_uri,
            evidence_json=evidence,
            decision=DECISION_HOLD,
            action_taken=ACTION_NO_STATE_CHANGE,
            outcome="VALIDATION_FAILED",
            otel_trace_id=trace_id,
            task_type_or_decision=DECISION_HOLD,
        )
        with conn:
            audit_inserted = _insert_audit_row(conn, row)
        return DECISION_HOLD, row.outcome, False, audit_inserted, str(error)

    # Queue mutation and its immutable audit record are one durability unit.
    # Any exception raised before the context exits rolls both statements back.
    with conn:
        action_inserted = False
        if decision.action_tool in (
            ACTION_INSERT_REVIEW_QUEUE,
            ACTION_INSERT_SLICE_ALERT,
        ):
            action_inserted = _insert_queue_task(conn, payload)
            outcome = "SUCCESS" if action_inserted else "SKIPPED_IDEMPOTENT"
        else:  # pragma: no cover - guarded by the allowlist above
            outcome = "SKIPPED_IDEMPOTENT"

        github_error: str | None = None
        if github_repo and github_token:
            ok, issue_url, github_error = _sandbox_issue(
                repo=github_repo,
                token=github_token,
                payload=payload,
            )
            if ok and issue_url:
                payload.evidence_json["sandbox_issue_url"] = issue_url
            elif not ok:
                outcome = "GITHUB_ISSUE_FAILED"

        row = build_audit_row(
            document_key=payload.document_key,
            batch_id=payload.batch_id,
            source_file_uri=payload.source_file_uri,
            evidence_json=payload.evidence_json,
            decision=decision.decision,
            action_taken=decision.action_tool,
            outcome=outcome,
            otel_trace_id=trace_id,
            task_type_or_decision=payload.task_type,
            task_id=payload.task_id,
        )
        audit_inserted = _insert_audit_row(conn, row)
    return (
        decision.decision,
        row.outcome,
        action_inserted,
        audit_inserted,
        github_error,
    )


def _load_candidates(
    conn: Any, max_actions: int, batch_id: str
) -> list[Mapping[str, Any]]:
    """Read active-batch candidates from the validated mart view."""
    from psycopg2.extras import RealDictCursor

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM marts.vw_agent_triage_candidates "
            "WHERE batch_id = %s ORDER BY candidate_key LIMIT %s",
            (batch_id, int(max_actions)),
        )
        return [dict(row) for row in cur.fetchall()]


def _run(max_actions: int) -> int:
    """Execute the quality-gated agent run."""
    from common.config import get_settings
    from common.db import connect, table_exists
    from common.lineage import emit_event
    from common.telemetry import new_trace_id, traced

    settings = get_settings()
    trace_id = new_trace_id()
    github_repo = settings.sandbox_github_repo
    github_token = settings.sandbox_github_token
    summary: Counter[str] = Counter()

    with traced(
        TRACE_JOB_NAME,
        trace_id=trace_id,
        attributes={"max_actions": max_actions, "batch_id": settings.batch_id},
    ):
        emit_event(
            TRACE_JOB_NAME,
            "START",
            inputs=["governance.dbt_run_metadata", "marts.vw_agent_triage_candidates"],
            outputs=[
                "governance.agent_review_queue",
                "governance.agent_action_log",
            ],
            run_id=trace_id,
            facets={"max_actions": max_actions},
        )
        try:
            with connect(settings) as conn:
                status = _latest_dbt_test_status(conn, settings.batch_id)
                if status != "PASS":
                    inserted = _insert_blocked_row(
                        conn,
                        batch_id=settings.batch_id,
                        status=status,
                        trace_id=trace_id,
                    )
                    summary["blocked"] = 1
                    summary["blocked_inserted"] = int(inserted)
                    print(
                        "BLOCKED_DQ_FAIL: latest dbt test status is "
                        f"{status or 'MISSING'}, expected PASS. "
                        f"Audit row {'inserted' if inserted else 'already present'}; "
                        f"trace_id={trace_id}"
                    )
                    emit_event(
                        TRACE_JOB_NAME,
                        "COMPLETE",
                        inputs=["governance.dbt_run_metadata"],
                        outputs=["governance.agent_action_log"],
                        run_id=trace_id,
                        facets={"blocked": True, "status": status or "MISSING"},
                    )
                    return 0

                if not table_exists(
                    conn, "marts", "vw_agent_triage_candidates"
                ):
                    print(
                        "ERROR: marts.vw_agent_triage_candidates is missing. "
                        "Run the dbt marts models (pipeline -> dbt_run) before "
                        "the triage agent.",
                        file=sys.stderr,
                    )
                    emit_event(
                        TRACE_JOB_NAME,
                        "COMPLETE",
                        inputs=["marts"],
                        outputs=[],
                        run_id=trace_id,
                        facets={"error": "missing_candidates_view"},
                    )
                    return 1

                candidates = _load_candidates(
                    conn, max_actions, settings.batch_id
                )
                print(
                    f"Loaded {len(candidates)} triage candidates "
                    f"(max_actions={max_actions}, trace_id={trace_id})"
                )
                examples: list[dict[str, Any]] = []
                for candidate in candidates:
                    (
                        decision_name,
                        outcome,
                        action_inserted,
                        audit_inserted,
                        error,
                    ) = _process_candidate(
                        conn,
                        candidate,
                        trace_id=trace_id,
                        github_repo=github_repo,
                        github_token=github_token,
                    )
                    summary[f"decision:{decision_name}"] += 1
                    summary[f"outcome:{outcome}"] += 1
                    summary["actions_inserted"] += int(action_inserted)
                    if not action_inserted and decision_name != DECISION_HOLD:
                        summary["skipped_idempotent"] += 1
                    if not audit_inserted:
                        summary["audit_skipped_idempotent"] += 1
                    if error:
                        summary["errors"] += 1
                    if len(examples) < 5:
                        examples.append(
                            {
                                "candidate_key": candidate.get("candidate_key"),
                                "document_key": candidate.get("document_key"),
                                "decision": decision_name,
                                "action_taken": decide(candidate).action_tool,
                                "outcome": outcome,
                            }
                        )

                print("\nAgent decisions / outcomes")
                for key in sorted(summary):
                    print(f"  {key}: {summary[key]}")
                if examples:
                    print("\nExample audit rows (truncated)")
                    for example in examples:
                        print(f"  {json.dumps(example, default=str)}")
                emit_event(
                    TRACE_JOB_NAME,
                    "COMPLETE",
                    inputs=[
                        "governance.dbt_run_metadata",
                        "marts.vw_agent_triage_candidates",
                    ],
                    outputs=[
                        "governance.agent_review_queue",
                        "governance.agent_action_log",
                    ],
                    run_id=trace_id,
                    facets=dict(summary),
                )
                return 0
        except Exception as error:  # noqa: BLE001 - actionable CLI failure
            print(
                f"ERROR: triage agent failed ({type(error).__name__}): {error}\n"
                "Check PII_DB_HOST/PORT/NAME/USER/PASSWORD, that Postgres is up, "
                "and that dbt has created the governance/marts relations.",
                file=sys.stderr,
            )
            return 1


# ---------------------------------------------------------------------------
# Self-test / CLI
# ---------------------------------------------------------------------------


SYNTHETIC_CANDIDATES: tuple[dict[str, Any], ...] = (
    {
        "candidate_type": "OCR_VERIFICATION",
        "document_key": "doc-ocr",
        "batch_id": "batch-1",
        "ocr_confidence": 0.42,
        "source_file_uri": "local://unstructured/batch-1/a.pdf",
    },
    {
        "candidate_type": "REDACTION_REVIEW",
        "document_key": "doc-redact",
        "batch_id": "batch-1",
        "has_overlapping_spans": True,
        "entity_density": 3.1,
    },
    {
        "candidate_type": "SLICE_QUARANTINE_ALERT",
        "document_key": "doc-slice",
        "batch_id": "batch-1",
        "quarantine_rate": 0.03,
    },
    {
        "candidate_type": "VERIFIED_FALSE_POSITIVE",
        "document_key": "doc-hold",
        "batch_id": "batch-1",
        "ocr_confidence": 0.97,
    },
    {
        "candidate_type": "SOMETHING_UNEXPECTED",
        "document_key": "doc-unknown",
        "batch_id": "batch-1",
    },
)


def run_self_test() -> int:
    """Exercise the policy over synthetic candidates without touching the DB."""
    print("Triage policy self-test (no database, no network)")
    print(f"{'candidate_type':<28}{'decision':<32}{'task_type':<24}"
          f"{'action_tool':<24}{'priority':<10}")
    print("-" * 118)
    failures = 0
    for candidate in SYNTHETIC_CANDIDATES:
        result = decide(candidate)
        print(
            f"{str(candidate['candidate_type']):<28}"
            f"{result.decision:<32}"
            f"{str(result.task_type):<24}"
            f"{result.action_tool:<24}"
            f"{result.priority:<10}"
        )
        if result.action_tool not in ALLOWED_ACTION_TOOLS:
            failures += 1
        if result.decision not in ALLOWED_DECISIONS:
            failures += 1
    if failures:
        print(f"FAILED: {failures} policy invariant violations", file=sys.stderr)
        return 1
    print("OK: all decisions/actions are allowlisted")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the triage agent."""
    parser = argparse.ArgumentParser(
        description="Quality-gated, deterministic PII triage agent."
    )
    parser.add_argument(
        "--max-actions",
        type=int,
        default=None,
        help="Maximum candidates to inspect (default PII_AGENT_MAX_ACTIONS).",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the decision policy over synthetic candidates (no DB).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m agent.triage_agent``."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.self_test:
        return run_self_test()
    from common.config import get_settings

    max_actions = (
        int(args.max_actions)
        if args.max_actions is not None
        else get_settings().agent_max_actions
    )
    if max_actions <= 0:
        print("ERROR: --max-actions must be a positive integer", file=sys.stderr)
        return 1
    return _run(max_actions)


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
