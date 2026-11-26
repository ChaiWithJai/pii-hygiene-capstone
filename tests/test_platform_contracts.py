"""Contract tests for shared configuration and observability helpers."""

from __future__ import annotations

import ast
import json
from pathlib import Path

from common.config import get_settings
from common.telemetry import new_trace_id, traced

ROOT = Path(__file__).resolve().parent.parent


def test_settings_contract(monkeypatch) -> None:
    monkeypatch.setenv("PII_BATCH_ID", "unit_batch")
    monkeypatch.setenv("PII_DB_PORT", "5432")
    settings = get_settings()
    assert settings.batch_id == "unit_batch"
    assert settings.db_port == 5432
    assert "dbname=pii_lab" in settings.dsn
    assert settings.raw_dir.name == "raw"


def test_trace_id_is_32_hex_and_propagates() -> None:
    trace_id = new_trace_id()
    assert len(trace_id) == 32
    int(trace_id, 16)
    with traced("unit.stage", trace_id=trace_id, attributes={"rows": 3}) as active:
        assert active == trace_id


def test_airflow_dag_is_parseable_and_has_gates() -> None:
    dag_file = ROOT / "airflow" / "dags" / "pii_hygiene_pipeline.py"
    source = dag_file.read_text(encoding="utf-8")
    tree = ast.parse(source)
    task_ids = {
        node.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.keyword)
        and node.arg == "task_id"
        and isinstance(node.value, ast.Constant)
        and isinstance(node.value.value, str)
    }
    assert {
        "ensure_structured_batch",
        "generate_unstructured_docs",
        "ocr_extract",
        "load_raw",
        "dbt_run",
        "dbt_test",
        "ml_evaluate_detectors",
        "triage_agent",
        "pipeline_summary",
    } <= task_ids
    assert "dbt_test >> ml_eval >> triage" in source
    assert "governance.dbt_run_metadata" in source


def test_docker_compose_has_required_services() -> None:
    import yaml

    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    assert {"warehouse", "airflow-webserver", "airflow-scheduler", "streamlit"} <= set(
        compose["services"]
    )
    assert compose["services"]["warehouse"]["image"].startswith("postgres:16")


def test_spec_contracts_exist() -> None:
    spec = (ROOT / "docs" / "internal" / "SPEC.md").read_text(encoding="utf-8")
    assert "marts.vw_agent_triage_candidates" in spec
    assert "OPEN_OCR_VERIFICATION_TASK" in spec
    assert "quarantine_threshold" in spec


def test_env_example_has_no_real_secrets() -> None:
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "SANDBOX_GITHUB_TOKEN=\n" in example
    assert "gho_" not in example
    json.dumps({"ok": True})


def test_public_corpus_schema_variants_keep_ground_truth() -> None:
    from ingestion.build_batches import normalize_source_row

    cases = [
        {
            "uid": 12,
            "source_text": "Jane",
            "source_dataset": "asia-pacific",
            "privacy_mask": [{"label": "GIVENNAME", "start": 0, "end": 4, "value": "Jane"}],
        },
        {"tokens": ["Jane"], "ent_tags": ["B-NAME"], "labels": [2]},
    ]
    for row in cases:
        result = normalize_source_row(
            row, batch_id="schema_test", source_dataset="fixture/schema",
            source_format="JSONL", fallback_record_id="test:1",
        )
        assert result["raw_text"] == "Jane"
        assert result["source_dataset"] == "fixture/schema"
        assert result["spans_json"][0]["value"] == "Jane"
