"""Unit tests for the offline ML evaluation contract."""

from __future__ import annotations

from typing import Any

import pytest

from ml import evaluate_pii_detectors as evaluation


def _document(index: int, group: str) -> evaluation.EvaluatedDoc:
    text = f"Contact user{index}@example.com"
    start = text.index("user")
    return evaluation.EvaluatedDoc(
        document_key=f"doc-{index}",
        raw_text=text,
        language="en",
        domain="test",
        template_hash=group,
        modality="clean_structured_text",
        source_dataset="fixture",
        batch_id="batch-1",
        spans=[
            evaluation.Span(
                start=start,
                end=len(text),
                label="email",
                value=text[start:],
                privacy_tier="direct_identifier",
            )
        ],
        dominant_tier="direct_identifier",
    )


def test_regex_detector_returns_end_exclusive_offsets() -> None:
    text = "Email jane@example.com or call 212-555-0199. SSN 123-45-6789."
    spans = evaluation.detect_regex(text)
    values = {(span.label, text[span.start : span.end]) for span in spans}
    assert ("email", "jane@example.com") in values
    assert ("phone", "212-555-0199") in values
    assert ("ssn", "123-45-6789") in values


def test_span_matching_requires_exact_offsets_and_label() -> None:
    truth = [evaluation.Span(10, 20, "email")]
    exact = [evaluation.Span(10, 20, "email")]
    partial = [evaluation.Span(11, 20, "email")]
    wrong_label = [evaluation.Span(10, 20, "phone")]

    assert evaluation.count_matches(exact, truth) == evaluation.MatchCounts(tp=1)
    assert evaluation.count_matches(partial, truth) == evaluation.MatchCounts(fp=1, fn=1)
    counts, _ = evaluation.match_by_entity(wrong_label, truth)
    assert counts["email"].fn == 1
    assert counts["phone"].fp == 1


def test_grouped_split_has_no_template_leakage() -> None:
    documents = [_document(index, f"template-{index // 2}") for index in range(20)]
    splits = evaluation.split_documents(documents, seed=42)

    assert all(splits.values())
    group_sets = {
        name: {doc.template_hash for doc in members}
        for name, members in splits.items()
    }
    assert group_sets["train"].isdisjoint(group_sets["validation"])
    assert group_sets["train"].isdisjoint(group_sets["test"])
    assert group_sets["validation"].isdisjoint(group_sets["test"])


def test_three_group_split_uses_deterministic_fallback_without_leakage() -> None:
    groups = ["template-a", "template-b", "template-c"]
    splits = evaluation.grouped_split_indices(groups, seed=42)
    evaluation.assert_no_group_overlap(groups, splits)
    assert all(splits.values())


def test_bootstrap_ci_is_reproducible_and_contains_point_f1() -> None:
    tp = [1, 1, 0, 1]
    fp = [0, 1, 0, 0]
    fn = [0, 0, 1, 0]
    first = evaluation.bootstrap_f1_ci(tp, fp, fn, n_iterations=200, seed=7)
    second = evaluation.bootstrap_f1_ci(tp, fp, fn, n_iterations=200, seed=7)
    point = evaluation.MatchCounts(sum(tp), sum(fp), sum(fn)).f1
    assert first == second
    assert first[0] <= point <= first[1]


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed.append((sql, params))


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()
        self.committed = False

    def __enter__(self) -> _Connection:
        return self

    def __exit__(self, exc_type: object, *_args: object) -> None:
        self.committed = exc_type is None

    def cursor(self) -> _Cursor:
        return self.cursor_instance


def test_replace_run_rows_uses_connection_and_one_transaction(monkeypatch) -> None:
    connection = _Connection()
    calls: list[tuple[object, str, list[tuple[str, int]]]] = []

    def fake_execute_values(conn: object, sql: str, rows: object) -> int:
        materialized = list(rows)  # type: ignore[arg-type]
        calls.append((conn, sql, materialized))
        return len(materialized)

    monkeypatch.setattr("common.db.execute_values", fake_execute_values)
    written = evaluation._replace_run_rows(
        connection,
        "raw.ml_operational_raw",
        "run-1",
        ("eval_run_id", "docs_evaluated"),
        [("run-1", 3)],
    )

    assert written == 1
    assert connection.committed
    assert calls[0][0] is connection
    assert "DELETE FROM raw.ml_operational_raw" in connection.cursor_instance.executed[0][0]


def test_max_docs_must_be_positive(monkeypatch) -> None:
    with pytest.raises(evaluation.EvaluationError, match="positive"):
        evaluation.build_documents(object(), max_docs=0)
