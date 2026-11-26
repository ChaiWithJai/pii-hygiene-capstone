"""Policy, batch gate, idempotency, and atomic persistence tests."""

from __future__ import annotations

from typing import Any

import pytest

from agent import triage_agent


@pytest.mark.parametrize(
    ("candidate_type", "decision", "task_type", "action"),
    [
        (
            "OCR_VERIFICATION",
            triage_agent.DECISION_OCR,
            triage_agent.TASK_TYPE_OCR,
            triage_agent.ACTION_INSERT_REVIEW_QUEUE,
        ),
        (
            "REDACTION_REVIEW",
            triage_agent.DECISION_REDACTION,
            triage_agent.TASK_TYPE_REDACTION,
            triage_agent.ACTION_INSERT_REVIEW_QUEUE,
        ),
        (
            "SLICE_QUARANTINE_ALERT",
            triage_agent.DECISION_SLICE,
            triage_agent.TASK_TYPE_SLICE,
            triage_agent.ACTION_INSERT_SLICE_ALERT,
        ),
        (
            "VERIFIED_FALSE_POSITIVE",
            triage_agent.DECISION_HOLD,
            None,
            triage_agent.ACTION_NO_STATE_CHANGE,
        ),
    ],
)
def test_policy_matrix(
    candidate_type: str, decision: str, task_type: str | None, action: str
) -> None:
    result = triage_agent.decide(
        {
            "candidate_type": candidate_type,
            "document_key": "doc-1",
            "batch_id": "batch-1",
        }
    )
    assert (result.decision, result.task_type, result.action_tool) == (
        decision,
        task_type,
        action,
    )


def test_task_and_audit_share_stable_idempotency_key() -> None:
    candidate = {
        "candidate_type": "OCR_VERIFICATION",
        "document_key": "doc-1",
        "batch_id": "batch-1",
        "evidence_json": {"confidence": 0.4},
    }
    decision = triage_agent.decide(candidate)
    task = triage_agent.build_task_payload(candidate, decision)
    audit = triage_agent.build_audit_row(
        document_key=task.document_key,
        batch_id=task.batch_id,
        source_file_uri=None,
        evidence_json=task.evidence_json,
        decision=decision.decision,
        action_taken=decision.action_tool,
        outcome="SUCCESS",
        otel_trace_id="a" * 32,
        task_type_or_decision=task.task_type,
        task_id=task.task_id,
    )
    assert task.task_id == audit.action_id == audit.idempotency_key
    assert len(task.task_id) == 32


class _GateCursor:
    def __init__(self, row: tuple[str] | None = ("PASS",)) -> None:
        self.row = row
        self.sql = ""
        self.params: tuple[str, ...] | None = None

    def __enter__(self) -> _GateCursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: tuple[str, ...]) -> None:
        self.sql = sql
        self.params = params

    def fetchone(self) -> tuple[str] | None:
        return self.row


class _GateConnection:
    def __init__(self) -> None:
        self.cursor_instance = _GateCursor()

    def cursor(self) -> _GateCursor:
        return self.cursor_instance


def test_dq_gate_is_scoped_to_active_batch(monkeypatch) -> None:
    connection = _GateConnection()
    monkeypatch.setattr("common.db.table_exists", lambda *_args: True)
    status = triage_agent._latest_dbt_test_status(connection, "batch-active")
    assert status == "PASS"
    assert "WHERE batch_id = %s" in connection.cursor_instance.sql
    assert connection.cursor_instance.params == ("batch-active",)


class _TransactionalConnection:
    def __init__(self) -> None:
        self.active = False
        self.committed = False
        self.rolled_back = False

    def __enter__(self) -> _TransactionalConnection:
        assert not self.active
        self.active = True
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        self.active = False
        self.committed = exc_type is None
        self.rolled_back = exc_type is not None


def _candidate() -> dict[str, Any]:
    return {
        "candidate_type": "OCR_VERIFICATION",
        "document_key": "doc-atomic",
        "batch_id": "batch-atomic",
        "ocr_confidence": 0.4,
    }


def test_queue_and_audit_are_written_in_same_transaction(monkeypatch) -> None:
    connection = _TransactionalConnection()
    observed: list[str] = []

    def insert_queue(conn: _TransactionalConnection, _payload: object) -> bool:
        assert conn.active
        observed.append("queue")
        return True

    def insert_audit(conn: _TransactionalConnection, _row: object) -> bool:
        assert conn.active
        observed.append("audit")
        return True

    monkeypatch.setattr(triage_agent, "_insert_queue_task", insert_queue)
    monkeypatch.setattr(triage_agent, "_insert_audit_row", insert_audit)
    result = triage_agent._process_candidate(
        connection,
        _candidate(),
        trace_id="b" * 32,
        github_repo="",
        github_token="",
    )

    assert observed == ["queue", "audit"]
    assert result[1:4] == ("SUCCESS", True, True)
    assert connection.committed
    assert not connection.rolled_back


def test_audit_failure_rolls_back_queue_mutation(monkeypatch) -> None:
    connection = _TransactionalConnection()

    def insert_queue(conn: _TransactionalConnection, _payload: object) -> bool:
        assert conn.active
        return True

    def fail_audit(conn: _TransactionalConnection, _row: object) -> bool:
        assert conn.active
        raise RuntimeError("audit unavailable")

    monkeypatch.setattr(triage_agent, "_insert_queue_task", insert_queue)
    monkeypatch.setattr(triage_agent, "_insert_audit_row", fail_audit)
    with pytest.raises(RuntimeError, match="audit unavailable"):
        triage_agent._process_candidate(
            connection,
            _candidate(),
            trace_id="c" * 32,
            github_repo="",
            github_token="",
        )

    assert connection.rolled_back
    assert not connection.committed


def test_hold_audit_is_durably_committed(monkeypatch) -> None:
    connection = _TransactionalConnection()

    def insert_audit(conn: _TransactionalConnection, _row: object) -> bool:
        assert conn.active
        return True

    monkeypatch.setattr(triage_agent, "_insert_audit_row", insert_audit)
    result = triage_agent._process_candidate(
        connection,
        {
            "candidate_type": "VERIFIED_FALSE_POSITIVE",
            "document_key": "doc-hold",
            "batch_id": "batch-atomic",
        },
        trace_id="d" * 32,
        github_repo="",
        github_token="",
    )
    assert result[0] == triage_agent.DECISION_HOLD
    assert connection.committed
