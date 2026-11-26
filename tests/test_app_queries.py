"""Unit tests for the Streamlit reviewer app.

These tests import ``app.streamlit_app`` without Streamlit, a database, pandas
or plotly. If Streamlit is installed the module is imported normally; otherwise
the module-level guard leaves ``st`` as ``None`` and every helper under test is
pure Python. An AST fallback documents the same invariants if the import ever
fails for an environment reason.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
APP_PATH = REPO_ROOT / "app" / "streamlit_app.py"

try:
    from app import streamlit_app as app_module
except Exception:  # pragma: no cover - last-resort AST path
    app_module = None


def _module():
    if app_module is None:
        pytest.skip("app.streamlit_app could not be imported")
    return app_module


# ---------------------------------------------------------------------------
# URI resolution
# ---------------------------------------------------------------------------


def test_resolve_local_uri_joins_data_dir():
    module = _module()
    resolved = module.resolve_local_uri(
        "local://unstructured/batch_01/doc_001.pdf", "/srv/pii/data"
    )
    assert resolved == Path("/srv/pii/data/unstructured/batch_01/doc_001.pdf")


def test_resolve_local_uri_missing_file_is_path_but_not_exists(tmp_path):
    module = _module()
    resolved = module.resolve_local_uri("local://unstructured/x.pdf", tmp_path)
    assert resolved is not None
    assert resolved.exists() is False


def test_resolve_local_uri_existing_file(tmp_path):
    module = _module()
    target = tmp_path / "unstructured" / "batch_01"
    target.mkdir(parents=True)
    document = target / "doc_001.pdf"
    document.write_bytes(b"%PDF-1.4 minimal")
    resolved = module.resolve_local_uri(
        "local://unstructured/batch_01/doc_001.pdf", tmp_path
    )
    assert resolved == document.resolve()
    assert resolved.exists()


def test_resolve_local_uri_rejects_remote_uris():
    module = _module()
    assert module.resolve_local_uri("gs://bucket/doc.pdf", "/data") is None
    assert module.resolve_local_uri("s3://bucket/doc.pdf", "/data") is None
    assert module.resolve_local_uri("", "/data") is None
    assert module.resolve_local_uri(None, "/data") is None


def test_resolve_local_uri_blocks_path_traversal(tmp_path):
    module = _module()
    assert module.resolve_local_uri("local://../../etc/passwd", tmp_path) is None


def test_uri_for_path_round_trip(tmp_path):
    module = _module()
    target = tmp_path / "unstructured" / "batch_01" / "doc.png"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"png")
    uri = module.uri_for_path(target, tmp_path)
    assert uri == "local://unstructured/batch_01/doc.png"
    assert module.resolve_local_uri(uri, tmp_path) == target.resolve()


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def test_safe_float_and_int():
    module = _module()
    assert module.safe_float("0.25") == 0.25
    assert module.safe_float(None) == 0.0
    assert module.safe_float("not-a-number", -1.0) == -1.0
    assert module.safe_float(float("nan"), 7.0) == 7.0
    assert module.safe_int("1617718") == 1617718
    assert module.safe_int(None, 3) == 3
    assert module.safe_int("x", 5) == 5


def test_format_count():
    module = _module()
    assert module.format_count(1617718) == "1,617,718"
    assert module.format_count("1000000") == "1,000,000"
    assert module.format_count(None) == "0"


def test_format_ci():
    module = _module()
    assert module.format_ci(0.8, 0.7, 0.9) == "0.800 [0.700-0.900]"
    assert module.format_ci(0.81234, None, None) == "0.812"
    assert module.format_ci(None, 0.1, 0.2) == "n/a"


def test_format_timestamp():
    module = _module()
    assert module.format_timestamp(None) == "n/a"
    assert "2026-11-26" in module.format_timestamp("2026-11-26T06:15:00Z")
    assert (
        module.format_timestamp("2026-11-26T06:15:00+00:00")
        == "2026-11-26 06:15:00 UTC"
    )


def test_optional_and_like_filters():
    module = _module()
    assert module.optional_filter("All") is None
    assert module.optional_filter(None) is None
    assert module.optional_filter("pdf") == "pdf"
    assert module.like_filter("") is None
    assert module.like_filter("SSN") == "%SSN%"
    assert module.like_filter("50%") == "%50\\%%"


# ---------------------------------------------------------------------------
# Bbox helpers used by the traceability tab
# ---------------------------------------------------------------------------


def test_parse_bbox_accepts_list_and_json_string():
    module = _module()
    assert module.parse_bbox([1, 2, 3, 4]) == [1.0, 2.0, 3.0, 4.0]
    assert module.parse_bbox("[1, 2, 3, 4]") == [1.0, 2.0, 3.0, 4.0]
    assert module.parse_bbox(json.dumps([10, 20, 30, 40])) == [
        10.0,
        20.0,
        30.0,
        40.0,
    ]
    assert module.parse_bbox("not json") is None
    assert module.parse_bbox(None) is None
    assert module.parse_bbox([1, 2, 3]) is None


def test_parse_bbox_rejects_invalid_geometry():
    module = _module()
    assert module.parse_bbox([-1, 2, 3, 4]) is None
    assert module.parse_bbox([3, 2, 1, 4]) is None
    assert module.parse_bbox([1, 4, 3, 2]) is None
    assert module.parse_bbox([1, 2, float("inf"), 4]) is None


def test_scale_bbox_converts_200dpi_to_144dpi():
    module = _module()
    scaled = module.scale_bbox([200.0, 100.0, 400.0, 300.0], 200.0, 2.0)
    assert scaled == pytest.approx([144.0, 72.0, 288.0, 216.0])


def test_scale_bbox_rejects_bad_dpi():
    module = _module()
    with pytest.raises(ValueError):
        module.scale_bbox([1, 2, 3, 4], 0.0, 2.0)


# ---------------------------------------------------------------------------
# SQL safety and contract surface
# ---------------------------------------------------------------------------


def test_sql_constants_have_no_template_interpolation():
    module = _module()
    for sql in module.SQL_CONSTANTS:
        assert "{" not in sql, "user input must never be interpolated into SQL"
        assert "}" not in sql
        assert "f\"" not in sql


def test_filter_queries_use_bind_parameters():
    module = _module()
    assert "%(source_dataset)s" in module.DENSITY_DISTRIBUTION_SQL
    assert "%(language)s" in module.DENSITY_DISTRIBUTION_SQL
    assert "%(privacy_tier)s" in module.DENSITY_DISTRIBUTION_SQL
    assert "%(search_like)s" in module.EXTRACTION_LIST_SQL
    assert "%(task_id)s" in module.UPDATE_REVIEW_QUEUE_SQL
    assert module.EXTRACTION_DETAIL_SQL.count("%s") == 1


def test_update_sql_only_touches_review_columns():
    module = _module()
    sql = module.UPDATE_REVIEW_QUEUE_SQL
    assert "governance.agent_review_queue" in sql
    for column in ("status", "reviewed_by", "reviewed_at", "review_note"):
        assert column in sql
    assert "DELETE" not in sql.upper()
    assert "INSERT" not in sql.upper()


def test_required_schema_qualified_relations_exist():
    module = _module()
    combined = "\n".join(module.SQL_CONSTANTS)
    for relation in (
        "marts.fct_pii_documents",
        "marts.fct_pii_spans",
        "marts.fct_unstructured_extractions",
        "marts.dim_entity_type",
        "marts.fct_ml_eval_slices",
        "marts.fct_ml_confusion",
        "marts.fct_ml_operational",
        "quarantine.pii_quarantined_documents",
        "governance.dbt_run_metadata",
        "governance.pipeline_runs",
        "governance.agent_action_log",
        "governance.agent_review_queue",
    ):
        assert relation in combined, f"missing relation: {relation}"


def test_document_grain_proof_query_uses_distinct_counts():
    module = _module()
    sql = module.DOC_GRAIN_PROOF_SQL
    assert sql.count("COUNT(DISTINCT document_key)") == 2
    assert "TOTAL_ALL_SOURCES" in sql
    assert "GROUP BY 1" in sql
    assert "source_format" not in sql


def test_cache_key_uses_latest_dbt_run_by_start_time():
    module = _module()
    assert "ORDER BY dbt_run_started_at DESC" in module.CACHE_KEY_SQL
    assert "MAX(run_id)" not in module.CACHE_KEY_SQL


def test_airflow_url_uses_documented_local_port():
    module = _module()
    assert module.AIRFLOW_URL == "http://localhost:8088"


def test_required_tab_labels():
    module = _module()
    labels = list(module.TAB_LABELS)
    assert len(labels) == 4
    joined = " ".join(labels).lower()
    assert "1m+" in joined
    assert "pipeline" in joined
    assert "ml" in joined
    assert "traceability" in joined
    assert "review queue" in joined


def test_dbt_badge_labels_are_textual():
    module = _module()
    assert module.dbt_badge("PASS") == ("PASS", "green")
    assert module.dbt_badge("FAIL") == ("FAIL", "red")
    assert module.dbt_badge(None)[0] == "NO DATA"


def test_redact_secrets():
    module = _module()
    redacted = module.redact_secrets(
        "connection failed password=super_secret host=db"
    )
    assert "super_secret" not in redacted
    assert "password=***" in redacted


# ---------------------------------------------------------------------------
# AST guardrails (do not execute the app)
# ---------------------------------------------------------------------------


def _parse_app() -> ast.Module:
    return ast.parse(APP_PATH.read_text(encoding="utf-8"))


def test_streamlit_import_is_guarded():
    tree = _parse_app()
    guarded = any(
        isinstance(node, ast.Try)
        and any(
            isinstance(handler.type, ast.Name) and handler.type.id == "ModuleNotFoundError"
            for handler in node.handlers
        )
        for node in ast.walk(tree)
    )
    assert guarded, "streamlit import must be wrapped in try/except"


def test_main_is_guarded_by_name_main():
    tree = _parse_app()
    guarded = False
    for node in tree.body:
        if isinstance(node, ast.If):
            test = node.test
            if (
                isinstance(test, ast.Compare)
                and isinstance(test.left, ast.Name)
                and test.left.id == "__name__"
            ):
                assert any(
                    isinstance(statement, ast.Expr)
                    and isinstance(statement.value, ast.Call)
                    and isinstance(statement.value.func, ast.Name)
                    and statement.value.func.id == "main"
                    for statement in node.body
                )
                guarded = True
    assert guarded, "database/Streamlit work must run under main()"


def test_sql_assignments_are_not_fstrings():
    tree = _parse_app()
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        names = [target.id for target in node.targets if isinstance(target, ast.Name)]
        if not any(name.endswith("_SQL") for name in names):
            continue
        assert not isinstance(node.value, ast.JoinedStr), f"{names} uses f-string SQL"


def test_no_hardcoded_admin_password_in_source():
    source = APP_PATH.read_text(encoding="utf-8")
    assert "pii_admin_pw" not in source
    assert "postgres:postgres" not in source
