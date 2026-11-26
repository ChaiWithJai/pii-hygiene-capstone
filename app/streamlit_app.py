"""PII Hygiene Reviewer: four-tab Streamlit frontend.

The app reads the validated marts with a least-privilege ``reviewer`` role and
may update only the status columns of ``governance.agent_review_queue``.
Every read is parameterized and cached for 300 seconds; the cache key is
derived from the newest ``marts.fct_pii_documents.ingested_at`` and the latest
dbt run id so a pipeline run invalidates cached results. A missing table or an
empty warehouse degrades to an actionable "run the pipeline" callout instead
of a traceback.

Run locally:

    streamlit run app/streamlit_app.py

Database connection settings are read from the same environment contract as
the rest of the stack (see ``common/config.py`` and ``.env.example``).
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Make the repository root importable when Streamlit is launched from app/.
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:  # Streamlit is optional for unit tests (they import this module offline).
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - exercised in CI without deps
    st = None  # type: ignore[assignment]


APP_TITLE = "PII Hygiene Reviewer"
GRAIN_STATEMENT = (
    "1 row per source document; span facts are separate to prevent join fan-out"
)
TARGET_DOCUMENTS = 1_000_000
AIRFLOW_URL = "http://localhost:8088"
ALL_OPTION = "All"
LOCAL_URI_PREFIX = "local://"
PAGE_DPI = 200.0
RENDER_ZOOM = 2.0
MAX_FILTER_ROWS = 20_000

TAB_LABELS = (
    "Pipeline health & 1M+ proof",
    "Corpus risk & ML slices",
    "High-variety source traceability",
    "Agent actions & review queue",
)

# ---------------------------------------------------------------------------
# SQL. Static text with named bind parameters only: user input never reaches
# the SQL string. Optional filters are expressed as "(%(param)s IS NULL OR ...)"
# so one prepared statement serves every filter combination.
# ---------------------------------------------------------------------------

CACHE_KEY_SQL = """
SELECT
    (SELECT MAX(ingested_at)::text FROM marts.fct_pii_documents) AS last_refresh,
    (
        SELECT run_id
        FROM governance.dbt_run_metadata
        ORDER BY dbt_run_started_at DESC
        LIMIT 1
    ) AS dbt_run_id
"""

DOC_GRAIN_COUNT_SQL = """
SELECT
    COUNT(DISTINCT document_key) AS unique_documents,
    MAX(ingested_at)             AS last_refresh
FROM marts.fct_pii_documents
"""

# Mirrors the RFC proof query; the local warehouse is the default database, so
# the RFC's "pii_lab.marts..." is expressed schema-qualified here.
DOC_GRAIN_PROOF_SQL = """
SELECT
    source_dataset,
    COUNT(DISTINCT document_key) AS unique_documents,
    MIN(ingested_at)             AS first_ingested_at,
    MAX(ingested_at)             AS last_refreshed_at
FROM marts.fct_pii_documents
GROUP BY 1
UNION ALL
SELECT
    'TOTAL_ALL_SOURCES'          AS source_dataset,
    COUNT(DISTINCT document_key) AS unique_documents,
    MIN(ingested_at)             AS first_ingested_at,
    MAX(ingested_at)             AS last_refreshed_at
FROM marts.fct_pii_documents
ORDER BY unique_documents DESC
"""

DBT_STATUS_SQL = """
SELECT
    run_id,
    batch_id,
    test_status,
    tests_passed,
    tests_failed,
    dbt_run_started_at,
    dbt_run_finished_at
FROM governance.dbt_run_metadata
ORDER BY dbt_run_started_at DESC
LIMIT 1
"""

QUARANTINE_TOTAL_SQL = """
SELECT COUNT(*) AS quarantined_rows
FROM quarantine.pii_quarantined_documents
"""

QUARANTINE_BY_REASON_SQL = """
SELECT
    failure_reason,
    COUNT(*) AS quarantined_rows,
    COUNT(DISTINCT batch_id) AS affected_batches
FROM quarantine.pii_quarantined_documents
GROUP BY 1
ORDER BY quarantined_rows DESC
"""

PIPELINE_RUNS_SQL = """
SELECT
    batch_id,
    stage,
    status,
    started_at,
    finished_at,
    EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds
FROM governance.pipeline_runs
ORDER BY started_at DESC
LIMIT 100
"""

FILTER_SOURCE_OPTIONS_SQL = """
SELECT DISTINCT source_dataset
FROM marts.fct_pii_documents
ORDER BY 1
"""

FILTER_LANGUAGE_OPTIONS_SQL = """
SELECT DISTINCT language
FROM marts.fct_pii_documents
WHERE language IS NOT NULL
ORDER BY 1
"""

FILTER_TIER_OPTIONS_SQL = """
SELECT DISTINCT privacy_tier
FROM marts.fct_pii_spans
WHERE privacy_tier IS NOT NULL
ORDER BY 1
"""

DENSITY_DISTRIBUTION_SQL = """
SELECT f.entity_density
FROM marts.fct_pii_documents AS f
WHERE (%(source_dataset)s IS NULL OR f.source_dataset = %(source_dataset)s)
  AND (%(language)s IS NULL OR f.language = %(language)s)
  AND (
        %(privacy_tier)s IS NULL
        OR EXISTS (
            SELECT 1
            FROM marts.fct_pii_spans AS s
            WHERE s.document_key = f.document_key
              AND s.privacy_tier = %(privacy_tier)s
        )
      )
  AND f.entity_density IS NOT NULL
LIMIT %(row_limit)s
"""

TIER_DISTRIBUTION_SQL = """
SELECT
    s.privacy_tier,
    COUNT(*)                    AS span_count,
    COUNT(DISTINCT s.document_key) AS document_count
FROM marts.fct_pii_spans AS s
JOIN marts.fct_pii_documents AS f ON f.document_key = s.document_key
WHERE (%(source_dataset)s IS NULL OR f.source_dataset = %(source_dataset)s)
  AND (%(language)s IS NULL OR f.language = %(language)s)
  AND (%(privacy_tier)s IS NULL OR s.privacy_tier = %(privacy_tier)s)
GROUP BY 1
ORDER BY span_count DESC
"""

QUARANTINE_FILTERED_SQL = """
SELECT
    failure_reason,
    COUNT(*) AS quarantined_rows
FROM quarantine.pii_quarantined_documents
WHERE (%(source_dataset)s IS NULL OR source_dataset = %(source_dataset)s)
GROUP BY 1
ORDER BY quarantined_rows DESC
"""

ML_SLICE_TYPES_SQL = """
SELECT DISTINCT slice_type
FROM marts.fct_ml_eval_slices
WHERE eval_run_id = (SELECT MAX(eval_run_id) FROM marts.fct_ml_eval_slices)
ORDER BY 1
"""

ML_EVAL_SLICES_SQL = """
SELECT
    eval_run_id,
    model_name,
    slice_type,
    slice_value,
    entity_type,
    precision,
    recall,
    f1,
    pr_auc,
    support,
    ci_low_f1,
    ci_high_f1,
    evaluated_at
FROM marts.fct_ml_eval_slices
WHERE eval_run_id = (SELECT MAX(eval_run_id) FROM marts.fct_ml_eval_slices)
  AND (%(slice_type)s IS NULL OR slice_type = %(slice_type)s)
ORDER BY model_name, slice_type, slice_value, entity_type
LIMIT %(row_limit)s
"""

ML_CONFUSION_SQL = """
SELECT
    model_name,
    slice_type,
    slice_value,
    entity_type,
    tp,
    fp,
    fn,
    tn
FROM marts.fct_ml_confusion
WHERE eval_run_id = (SELECT MAX(eval_run_id) FROM marts.fct_ml_confusion)
ORDER BY model_name, slice_type, slice_value, entity_type
LIMIT %(row_limit)s
"""

ML_OPERATIONAL_SQL = """
SELECT
    eval_run_id,
    model_name,
    median_latency_ms,
    p95_latency_ms,
    docs_per_sec,
    peak_ram_mb,
    artifact_size_mb,
    docs_evaluated,
    evaluated_at
FROM marts.fct_ml_operational
WHERE eval_run_id = (SELECT MAX(eval_run_id) FROM marts.fct_ml_operational)
ORDER BY model_name
"""

EXTRACTION_LIST_SQL = """
SELECT
    e.extraction_key,
    e.document_key,
    e.source_file_uri,
    e.file_type,
    e.page_number,
    e.ocr_confidence,
    e.extracted_value,
    e.raw_entity_label,
    e.privacy_tier,
    e.has_high_risk_pii,
    d.canonical_entity_label
FROM marts.fct_unstructured_extractions AS e
LEFT JOIN marts.dim_entity_type AS d
       ON d.entity_type_key = e.entity_type_key
WHERE (%(file_type)s IS NULL OR e.file_type = %(file_type)s)
  AND (%(min_confidence)s IS NULL OR e.ocr_confidence >= %(min_confidence)s)
  AND (
        %(search_like)s IS NULL
        OR e.extracted_value ILIKE %(search_like)s
        OR e.raw_entity_label ILIKE %(search_like)s
        OR e.source_file_uri ILIKE %(search_like)s
      )
ORDER BY e.ocr_confidence ASC, e.ingested_at DESC
LIMIT %(row_limit)s
"""

EXTRACTION_DETAIL_SQL = """
SELECT
    e.extraction_key,
    e.document_key,
    e.source_file_uri,
    e.file_type,
    e.page_number,
    e.bbox_json,
    e.ocr_confidence,
    e.extracted_value,
    e.raw_entity_label,
    e.privacy_tier,
    e.has_high_risk_pii,
    e.needs_ocr_verification,
    e.extracted_text,
    e.language,
    e.domain,
    e.batch_id,
    d.canonical_entity_label,
    d.description AS entity_description
FROM marts.fct_unstructured_extractions AS e
LEFT JOIN marts.dim_entity_type AS d
       ON d.entity_type_key = e.entity_type_key
WHERE e.extraction_key = %s
"""

ACTION_LOG_SQL = """
SELECT
    executed_at,
    action_id,
    task_id,
    document_key,
    decision,
    action_taken,
    outcome,
    otel_trace_id,
    source_file_uri,
    batch_id,
    evidence_json
FROM governance.agent_action_log
WHERE (%(decision)s IS NULL OR decision = %(decision)s)
  AND (%(outcome)s IS NULL OR outcome = %(outcome)s)
ORDER BY executed_at DESC
LIMIT %(row_limit)s
"""

ACTION_COUNTS_SQL = """
SELECT
    decision,
    outcome,
    COUNT(*) AS actions
FROM governance.agent_action_log
WHERE (%(decision)s IS NULL OR decision = %(decision)s)
  AND (%(outcome)s IS NULL OR outcome = %(outcome)s)
GROUP BY 1, 2
ORDER BY actions DESC
"""

ACTION_DECISION_OPTIONS_SQL = """
SELECT DISTINCT decision
FROM governance.agent_action_log
ORDER BY 1
"""

ACTION_OUTCOME_OPTIONS_SQL = """
SELECT DISTINCT outcome
FROM governance.agent_action_log
ORDER BY 1
"""

OPEN_REVIEW_QUEUE_SQL = """
SELECT
    task_id,
    task_type,
    document_key,
    source_file_uri,
    batch_id,
    priority,
    status,
    assigned_role,
    evidence_json,
    created_by,
    created_at
FROM governance.agent_review_queue
WHERE status = 'OPEN'
ORDER BY
    CASE priority
        WHEN 'CRITICAL' THEN 0
        WHEN 'HIGH' THEN 1
        WHEN 'MEDIUM' THEN 2
        ELSE 3
    END,
    created_at ASC
LIMIT 100
"""

QUEUE_STATUS_COUNTS_SQL = """
SELECT status, COUNT(*) AS tasks
FROM governance.agent_review_queue
GROUP BY 1
ORDER BY tasks DESC
"""

UPDATE_REVIEW_QUEUE_SQL = """
UPDATE governance.agent_review_queue
SET status       = %(status)s,
    reviewed_by  = %(reviewed_by)s,
    reviewed_at  = now(),
    review_note  = %(review_note)s
WHERE task_id = %(task_id)s
  AND status = 'OPEN'
"""

# Module-level alias used by tests and docs.
SQL_CONSTANTS = (
    CACHE_KEY_SQL,
    DOC_GRAIN_COUNT_SQL,
    DOC_GRAIN_PROOF_SQL,
    DBT_STATUS_SQL,
    QUARANTINE_TOTAL_SQL,
    QUARANTINE_BY_REASON_SQL,
    PIPELINE_RUNS_SQL,
    FILTER_SOURCE_OPTIONS_SQL,
    FILTER_LANGUAGE_OPTIONS_SQL,
    FILTER_TIER_OPTIONS_SQL,
    DENSITY_DISTRIBUTION_SQL,
    TIER_DISTRIBUTION_SQL,
    QUARANTINE_FILTERED_SQL,
    ML_SLICE_TYPES_SQL,
    ML_EVAL_SLICES_SQL,
    ML_CONFUSION_SQL,
    ML_OPERATIONAL_SQL,
    EXTRACTION_LIST_SQL,
    EXTRACTION_DETAIL_SQL,
    ACTION_LOG_SQL,
    ACTION_COUNTS_SQL,
    ACTION_DECISION_OPTIONS_SQL,
    ACTION_OUTCOME_OPTIONS_SQL,
    OPEN_REVIEW_QUEUE_SQL,
    QUEUE_STATUS_COUNTS_SQL,
    UPDATE_REVIEW_QUEUE_SQL,
)

_LAST_DB_ERROR: str | None = None


# ---------------------------------------------------------------------------
# Pure helpers (unit tested without a database or Streamlit).
# ---------------------------------------------------------------------------


def safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce a warehouse value to float, falling back to ``default``."""
    if value is None:
        return default
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result):
        return default
    return result


def safe_int(value: Any, default: int = 0) -> int:
    """Coerce a warehouse value to int, falling back to ``default``."""
    if value is None:
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def format_count(value: Any) -> str:
    """Format an integer count with thousands separators."""
    return f"{safe_int(value):,}"


def format_timestamp(value: Any, fmt: str = "%Y-%m-%d %H:%M:%S UTC") -> str:
    """Format a timestamp-like value as a UTC string; never raises."""
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return value
    else:
        return str(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).strftime(fmt)


def format_ci(f1: Any, low: Any, high: Any, digits: int = 3) -> str:
    """Format an F1 score with its 95 percent confidence interval."""
    nan = float("nan")
    score = safe_float(f1, nan)
    low_value = safe_float(low, nan)
    high_value = safe_float(high, nan)
    if math.isnan(score):
        return "n/a"
    if math.isnan(low_value) or math.isnan(high_value):
        return f"{score:.{digits}f}"
    return f"{score:.{digits}f} [{low_value:.{digits}f}-{high_value:.{digits}f}]"


def format_confidence(value: Any, digits: int = 3) -> str:
    """Format an OCR confidence in [0, 1] as a decimal string."""
    return f"{safe_float(value):.{digits}f}"


def format_percent(value: Any, digits: int = 2) -> str:
    """Format a fraction as a percentage string."""
    return f"{safe_float(value) * 100:.{digits}f}%"


def parse_bbox(value: Any) -> list[float] | None:
    """Parse a bbox from jsonb/list forms into ``[x1, y1, x2, y2]``."""
    if value is None:
        return None
    candidate: Any = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except (TypeError, ValueError):
            return None
    if not isinstance(candidate, list | tuple) or len(candidate) != 4:
        return None
    try:
        parsed = [float(part) for part in candidate]
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(part) for part in parsed):
        return None
    x1, y1, x2, y2 = parsed
    if min(parsed) < 0 or x2 < x1 or y2 < y1:
        return None
    return parsed


def scale_bbox(
    bbox: Sequence[float],
    page_dpi: float = PAGE_DPI,
    zoom: float = RENDER_ZOOM,
) -> list[float]:
    """Scale a 200 DPI bbox into rendered pixel coordinates.

    PyMuPDF's base coordinate system is 72 points per inch. Rendering with
    ``zoom=2`` yields 144 DPI, so a bbox captured at 200 DPI must be multiplied
    by ``zoom * 72 / page_dpi`` to land on the correct pixels.
    """
    if page_dpi <= 0:
        raise ValueError("page_dpi must be positive")
    scale = (zoom * 72.0) / page_dpi
    return [float(coordinate) * scale for coordinate in bbox]


def resolve_local_uri(uri: Any, data_dir: str | Path) -> Path | None:
    """Resolve ``local://<relative path>`` against ``PII_DATA_DIR``.

    Returns ``None`` for object-storage URIs or paths that escape the data
    directory. The returned path may not exist; callers check ``exists()``.
    """
    if not uri or not isinstance(uri, str):
        return None
    if not uri.startswith(LOCAL_URI_PREFIX):
        return None
    relative = uri[len(LOCAL_URI_PREFIX) :].lstrip("/")
    if not relative:
        return None
    base = Path(data_dir).expanduser().resolve()
    candidate = (base / relative).resolve()
    try:
        candidate.relative_to(base)
    except ValueError:
        return None
    return candidate


def uri_for_path(path: str | Path, data_dir: str | Path) -> str:
    """Inverse of :func:`resolve_local_uri` for manifest/debug display."""
    base = Path(data_dir).expanduser().resolve()
    target = Path(path).expanduser().resolve()
    try:
        relative = target.relative_to(base)
    except ValueError:
        return target.as_uri()
    return f"{LOCAL_URI_PREFIX}{relative.as_posix()}"


def optional_filter(value: Any, all_value: str = ALL_OPTION) -> str | None:
    """Map the UI "All" sentinel and blanks to ``None`` (SQL NULL)."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text == all_value:
        return None
    return text


def like_filter(value: Any) -> str | None:
    """Build a LIKE pattern for a bind parameter (never interpolated)."""
    text = optional_filter(value, all_value="")
    if text is None:
        return None
    escaped = text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def redact_secrets(message: Any) -> str:
    """Remove password-like fragments from an error message before display."""
    text = str(message)
    text = re.sub(r"(?i)password\s*=\s*\S+", "password=***", text)
    text = re.sub(r"(?i)(pii_db_password\s*[=:]\s*)\S+", r"\1***", text)
    return text[:400]


def dbt_badge(status: Any) -> tuple[str, str]:
    """Return (label, streamlit color) for a dbt test status."""
    normalized = str(status or "NONE").strip().upper()
    if normalized == "PASS":
        return "PASS", "green"
    if normalized == "FAIL":
        return "FAIL", "red"
    return "NO DATA", "gray"


def app_db_kwargs(
    user: str | None = None, password: str | None = None
) -> dict[str, Any]:
    """Connection kwargs for the app, defaulting to the reviewer role."""
    from common.config import get_settings

    settings = get_settings()
    return {
        "host": os.environ.get("PII_DB_HOST") or settings.db_host,
        "port": int(os.environ.get("PII_DB_PORT") or settings.db_port),
        "dbname": os.environ.get("PII_DB_NAME") or settings.db_name,
        "user": user or os.environ.get("PII_DB_USER") or "reviewer",
        "password": password or os.environ.get("PII_DB_PASSWORD") or "reviewer_pw",
        "connect_timeout": 5,
    }


def data_directory() -> Path:
    """Resolve ``PII_DATA_DIR`` the same way the workers do."""
    from common.config import get_settings

    explicit = os.environ.get("PII_DATA_DIR")
    if explicit:
        return Path(explicit)
    return get_settings().data_dir


def data_fingerprint(value: Any) -> str:
    """Stable cache-key fragment for a value returned by the database."""
    if value is None:
        return "none"
    if isinstance(value, list | tuple):
        return ",".join(data_fingerprint(part) for part in value)
    return str(value).strip() or "none"


# ---------------------------------------------------------------------------
# Database access. All reads funnel through cached_query so the TTL and the
# explicit cache key are applied uniformly.
# ---------------------------------------------------------------------------

if st is not None:
    _cache_data = st.cache_data(ttl=300, show_spinner=False)
else:  # pragma: no cover - test/import path

    def _cache_data(function: Any) -> Any:
        return function


@_cache_data
def cached_query(
    sql: str,
    params: dict[str, Any] | tuple[Any, ...] | None = None,
    cache_key: str = "",
) -> Any:
    """Cached wrapper around :func:`safe_df`; keyed by SQL, params and data."""
    return safe_df(sql, params)


def safe_df(
    sql: str,
    params: dict[str, Any] | tuple[Any, ...] | None = None,
    columns: Iterable[str] | None = None,
) -> Any:
    """Run a read-only query, returning an empty DataFrame on warehouse errors.

    Missing tables (fresh warehouse) and undefined columns (partially built
    dbt project) are treated as "no data yet" so the UI can show a callout
    instead of a traceback. The error is retained for a debug caption.
    """
    global _LAST_DB_ERROR
    try:
        import pandas as pd
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ModuleNotFoundError as exc:  # pragma: no cover - environment issue
        _LAST_DB_ERROR = f"missing dependency: {exc.name}"
        return _empty_frame(columns)

    try:
        connection = psycopg2.connect(**app_db_kwargs())
    except Exception as exc:  # noqa: BLE001 - surfaced in the UI, never raised
        _LAST_DB_ERROR = f"connection failed: {redact_secrets(exc)}"
        return _empty_frame(columns)

    try:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(sql, params)
            rows = cursor.fetchall()
        frame = pd.DataFrame(rows)
        if frame.empty:
            return _empty_frame(columns)
        return frame
    except Exception as exc:  # noqa: BLE001 - fresh warehouse is expected
        _LAST_DB_ERROR = f"query failed: {redact_secrets(exc)}"
        return _empty_frame(columns)
    finally:
        connection.close()


def _empty_frame(columns: Iterable[str] | None = None) -> Any:
    import pandas as pd

    return pd.DataFrame(columns=list(columns) if columns else [])


def last_db_error() -> str | None:
    """Most recent swallowed database error, for a collapsible debug caption."""
    return _LAST_DB_ERROR


def load_cache_key() -> str:
    """Cache key from MAX(ingested_at) and the latest dbt run id."""
    global _LAST_DB_ERROR
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ModuleNotFoundError:
        return "unavailable"

    try:
        connection = psycopg2.connect(**app_db_kwargs())
    except Exception as exc:  # noqa: BLE001
        _LAST_DB_ERROR = f"connection failed: {redact_secrets(exc)}"
        return "unavailable"

    try:
        connection.set_session(readonly=True, autocommit=True)
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(CACHE_KEY_SQL)
            row = cursor.fetchone() or {}
        return "|".join(
            data_fingerprint(row.get(column))
            for column in ("last_refresh", "dbt_run_id")
        )
    except Exception:  # noqa: BLE001 - marts may not exist yet
        return "empty"
    finally:
        connection.close()


def update_review_status(
    task_id: str, status: str, reviewer: str, note: str | None
) -> tuple[bool, str]:
    """Update one OPEN review task through the least-privilege reviewer role."""
    global _LAST_DB_ERROR
    if status not in {"APPROVED", "DISMISSED", "ESCALATED"}:
        return False, f"unsupported status: {status}"
    try:
        import psycopg2
    except ModuleNotFoundError:
        return False, "psycopg2 is not installed"

    try:
        connection = psycopg2.connect(**app_db_kwargs())
    except Exception as exc:  # noqa: BLE001
        message = f"connection failed: {redact_secrets(exc)}"
        _LAST_DB_ERROR = message
        return False, message

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                UPDATE_REVIEW_QUEUE_SQL,
                {
                    "status": status,
                    "reviewed_by": reviewer or "streamlit_reviewer",
                    "review_note": note,
                    "task_id": task_id,
                },
            )
            updated = cursor.rowcount
        connection.commit()
    except Exception as exc:  # noqa: BLE001
        connection.rollback()
        message = f"update failed: {redact_secrets(exc)}"
        _LAST_DB_ERROR = message
        return False, message
    finally:
        connection.close()

    if updated == 0:
        return False, "task was not OPEN or no longer exists"
    clear_cache()
    return True, f"{task_id} marked {status}"


def clear_cache() -> None:
    """Drop all cached query results."""
    if st is not None:
        st.cache_data.clear()


def query(
    sql: str,
    params: dict[str, Any] | None = None,
    cache_key: str = "",
    columns: Iterable[str] | None = None,
) -> Any:
    """Shortcut for a cached parameterized read."""
    frame = cached_query(sql, params, cache_key)
    if frame is None:
        return _empty_frame(columns)
    if frame.empty and columns is not None:
        return _empty_frame(columns)
    return frame


# ---------------------------------------------------------------------------
# Rendering helpers
# ---------------------------------------------------------------------------


def _pipeline_callout() -> None:
    if st is None:
        return
    st.warning(
        "No validated documents are visible yet. Run the pipeline to populate "
        "the marts, then press **Refresh data**.\n\n"
        "```bash\n"
        "cp .env.example .env\n"
        "docker compose up -d --build\n"
        "docker compose exec airflow-webserver "
        "airflow dags trigger pii_hygiene_pipeline\n"
        "```\n\n"
        "Host-only quick run: `scripts/run_local_pipeline.sh --quick`."
    )


def _metric_row(
    unique_documents: int, last_refresh: Any, dbt_row: Any, quarantined: int
) -> None:
    if st is None:
        return
    columns = st.columns(4)
    columns[0].metric(
        "Verified documents",
        format_count(unique_documents),
        delta=f"target {format_count(TARGET_DOCUMENTS)}",
        delta_color="off",
        help=GRAIN_STATEMENT,
    )
    columns[1].metric("Last data refresh", format_timestamp(last_refresh))
    status = None
    if dbt_row is not None and hasattr(dbt_row, "get"):
        status = dbt_row.get("test_status")
    label, color = dbt_badge(status)
    with columns[2]:
        st.markdown(f"**dbt tests** :{color}[{label}]")
        if dbt_row is not None and hasattr(dbt_row, "get") and status:
            st.caption(
                f"run {dbt_row.get('run_id')} | "
                f"passed {safe_int(dbt_row.get('tests_passed'))} | "
                f"failed {safe_int(dbt_row.get('tests_failed'))}"
            )
        else:
            st.caption("no dbt run recorded yet")
    columns[3].metric(
        "Quarantined rows",
        format_count(quarantined),
        help="Bad records blocked from the agent-facing marts.",
    )


def render_header(cache_key: str) -> None:
    """Render the KPI header shared by all tabs."""
    if st is None:
        return
    document_frame = query(
        DOC_GRAIN_COUNT_SQL,
        {},
        cache_key,
        columns=["unique_documents", "last_refresh"],
    )
    unique_documents = (
        safe_int(document_frame.iloc[0]["unique_documents"])
        if not document_frame.empty
        else 0
    )
    last_refresh = (
        document_frame.iloc[0]["last_refresh"] if not document_frame.empty else None
    )

    dbt_frame = query(
        DBT_STATUS_SQL,
        {},
        cache_key,
        columns=[
            "run_id",
            "batch_id",
            "test_status",
            "tests_passed",
            "tests_failed",
            "dbt_run_started_at",
            "dbt_run_finished_at",
        ],
    )
    dbt_row = None if dbt_frame.empty else dbt_frame.iloc[0]

    quarantine_frame = query(
        QUARANTINE_TOTAL_SQL, {}, cache_key, columns=["quarantined_rows"]
    )
    quarantined = (
        safe_int(quarantine_frame.iloc[0]["quarantined_rows"])
        if not quarantine_frame.empty
        else 0
    )

    st.title(APP_TITLE)
    st.caption(GRAIN_STATEMENT)
    _metric_row(unique_documents, last_refresh, dbt_row, quarantined)

    if unique_documents == 0:
        _pipeline_callout()

    if unique_documents >= TARGET_DOCUMENTS:
        st.success(
            f"1M+ proof satisfied: {format_count(unique_documents)} distinct "
            "document_key values at document grain (no span join fan-out)."
        )
    elif unique_documents > 0:
        st.info(
            f"{format_count(unique_documents)} documents validated so far. "
            f"The 1M+ target needs {format_count(TARGET_DOCUMENTS)}."
        )


def render_sidebar(cache_key: str) -> None:
    if st is None:
        return
    with st.sidebar:
        st.header("Data controls")
        if st.button("Refresh data", help="Clear cached query results and reload."):
            clear_cache()
            st.rerun()
        st.caption(f"Cache key: `{cache_key[:64]}`")
        kwargs = app_db_kwargs()
        st.caption(
            f"Warehouse: `{kwargs['host']}:{kwargs['port']}/"
            f"{kwargs['dbname']}` as `{kwargs['user']}` (read-mostly)"
        )
        error = last_db_error()
        if error:
            with st.expander("Last query note"):
                st.code(error, language="text")
        st.divider()
        st.markdown(
            f"[Open Airflow UI]({AIRFLOW_URL}) (admin / admin by default)"
        )
        st.caption("Tabs 1-4 mirror the four acceptance surfaces in the RFC.")


def render_pipeline_health(cache_key: str) -> None:
    """Tab 1: pipeline health and the 1M+ grain proof."""
    if st is None:
        return
    st.subheader("1M+ proof at document grain")
    proof = query(
        DOC_GRAIN_PROOF_SQL,
        {},
        cache_key,
        columns=[
            "source_dataset",
            "unique_documents",
            "first_ingested_at",
            "last_refreshed_at",
        ],
    )
    if proof.empty:
        st.info("The proof query returned no rows. Build the marts first.")
        _pipeline_callout()
    else:
        display = proof.copy()
        display["unique_documents"] = display["unique_documents"].map(format_count)
        display["first_ingested_at"] = display["first_ingested_at"].map(
            format_timestamp
        )
        display["last_refreshed_at"] = display["last_refreshed_at"].map(
            format_timestamp
        )
        st.dataframe(display, width="stretch", hide_index=True)
        st.caption(
            "Grain: one row per source document (`COUNT(DISTINCT document_key)`). "
            "Span facts live in `marts.fct_pii_spans` and are counted separately."
        )

    left, right = st.columns(2)
    with left:
        st.subheader("Pipeline stage status")
        runs = query(
            PIPELINE_RUNS_SQL,
            {},
            cache_key,
            columns=[
                "batch_id",
                "stage",
                "status",
                "started_at",
                "finished_at",
                "duration_seconds",
            ],
        )
        if runs.empty:
            st.info(
                "No `governance.pipeline_runs` rows yet. Each DAG stage writes "
                "one row per run."
            )
        else:
            display = runs.copy()
            display["started_at"] = display["started_at"].map(format_timestamp)
            display["finished_at"] = display["finished_at"].map(format_timestamp)
            display["duration_seconds"] = display["duration_seconds"].map(
                lambda value: f"{safe_float(value):.1f}s"
            )
            st.dataframe(display, width="stretch", hide_index=True)
    with right:
        st.subheader("Quarantine by failure reason")
        reasons = query(
            QUARANTINE_BY_REASON_SQL,
            {},
            cache_key,
            columns=["failure_reason", "quarantined_rows", "affected_batches"],
        )
        if reasons.empty:
            st.info(
                "Quarantine is empty. That is expected on a clean batch. Use "
                "`python -m ingestion.inject_bad_records --batch-id "
                "test_quarantine_01` to prove the split-routing path."
            )
        else:
            display = reasons.copy()
            display["quarantined_rows"] = display["quarantined_rows"].map(
                format_count
            )
            st.dataframe(display, width="stretch", hide_index=True)

    st.subheader("Orchestration")
    st.markdown(
        f"- Airflow UI: [{AIRFLOW_URL}]({AIRFLOW_URL}) (DAG "
        "`pii_hygiene_pipeline`, schedule `None`, trigger manually).\n"
        "- CLI trigger: `docker compose exec airflow-webserver airflow dags "
        "trigger pii_hygiene_pipeline`.\n"
        "- DAG params: `batch_id`, `limit_per_source`, `doc_count`, "
        "`max_agent_actions`, `eval_max_docs`."
    )


def _filter_controls(cache_key: str) -> dict[str, Any]:
    """Render the tab-2 filters and return SQL bind parameters."""
    if st is None:
        return {"source_dataset": None, "language": None, "privacy_tier": None}

    sources = query(
        FILTER_SOURCE_OPTIONS_SQL, {}, cache_key, columns=["source_dataset"]
    )
    languages = query(
        FILTER_LANGUAGE_OPTIONS_SQL, {}, cache_key, columns=["language"]
    )
    tiers = query(FILTER_TIER_OPTIONS_SQL, {}, cache_key, columns=["privacy_tier"])

    def options(frame: Any, column: str) -> list[str]:
        if frame.empty:
            return [ALL_OPTION]
        values = [str(value) for value in frame[column].dropna().tolist()]
        return [ALL_OPTION] + values

    columns = st.columns(3)
    source = columns[0].selectbox(
        "Source dataset", options(sources, "source_dataset")
    )
    language = columns[1].selectbox("Language", options(languages, "language"))
    tier = columns[2].selectbox("Privacy tier", options(tiers, "privacy_tier"))
    return {
        "source_dataset": optional_filter(source),
        "language": optional_filter(language),
        "privacy_tier": optional_filter(tier),
    }


def render_corpus_risk(cache_key: str) -> None:
    """Tab 2: corpus risk, quarantine and ML slice analytics."""
    if st is None:
        return
    st.subheader("Corpus risk filters")
    filters = _filter_controls(cache_key)
    params = dict(filters)
    params["row_limit"] = MAX_FILTER_ROWS

    left, right = st.columns(2)
    with left:
        st.subheader("Entity density (spans per 100 characters)")
        density = query(
            DENSITY_DISTRIBUTION_SQL,
            params,
            cache_key,
            columns=["entity_density"],
        )
        if density.empty:
            st.info(
                "No entity density values for this filter. Widen the filters "
                "or run the dbt marts."
            )
        else:
            import plotly.express as px

            values = density["entity_density"].astype(float)
            figure = px.histogram(
                values,
                nbins=40,
                labels={"value": "spans per 100 characters", "count": "documents"},
                title=f"{format_count(len(values))} validated documents",
            )
            figure.add_vline(
                x=2.5,
                line_dash="dash",
                line_color="orange",
                annotation_text="agent density trigger 2.5",
                annotation_position="top right",
            )
            figure.update_layout(
                showlegend=False, height=380, margin={"t": 60, "b": 40}
            )
            st.plotly_chart(figure, width="stretch")
            st.caption(
                "Density = span_count * 100 / NULLIF(text_char_length, 0). "
                "Documents above 2.5 are candidates for redaction review."
            )
    with right:
        st.subheader("Privacy tier distribution")
        tiers = query(
            TIER_DISTRIBUTION_SQL,
            params,
            cache_key,
            columns=["privacy_tier", "span_count", "document_count"],
        )
        if tiers.empty:
            st.info("No span rows for this filter combination.")
        else:
            import plotly.express as px

            figure = px.bar(
                tiers,
                x="privacy_tier",
                y="span_count",
                text="span_count",
                hover_data={"document_count": True, "span_count": ":,"},
                labels={
                    "privacy_tier": "privacy tier",
                    "span_count": "validated spans",
                },
            )
            figure.update_layout(height=380, margin={"t": 30, "b": 40})
            st.plotly_chart(figure, width="stretch")
            st.dataframe(tiers, width="stretch", hide_index=True)

    st.subheader("Quarantine counts for this filter")
    quarantine = query(
        QUARANTINE_FILTERED_SQL,
        params,
        cache_key,
        columns=["failure_reason", "quarantined_rows"],
    )
    if quarantine.empty:
        st.info(
            "No quarantined rows for the selected source. Quarantine is keyed "
            "by batch and failure reason; language and tier filters do not "
            "apply to rejected rows."
        )
    else:
        st.dataframe(quarantine, width="stretch", hide_index=True)

    st.divider()
    st.subheader("ML evaluation slices (95 percent bootstrap CI)")

    slice_types = query(
        ML_SLICE_TYPES_SQL, {}, cache_key, columns=["slice_type"]
    )
    if slice_types.empty:
        st.info(
            "No evaluation results yet. Run:\n\n"
            "```bash\npython -m ml.evaluate_pii_detectors --eval-run-id "
            "eval_manual_001\n```"
        )
        return
    available = [str(value) for value in slice_types["slice_type"].tolist()]
    default_index = available.index("overall") if "overall" in available else 0
    selected_slice = st.selectbox(
        "Slice type", available, index=default_index, key="ml_slice_type"
    )
    slice_params = {"slice_type": selected_slice, "row_limit": 2000}
    slices = query(
        ML_EVAL_SLICES_SQL,
        slice_params,
        cache_key,
        columns=[
            "eval_run_id",
            "model_name",
            "slice_type",
            "slice_value",
            "entity_type",
            "precision",
            "recall",
            "f1",
            "pr_auc",
            "support",
            "ci_low_f1",
            "ci_high_f1",
            "evaluated_at",
        ],
    )
    if slices.empty:
        st.info("The selected slice has no rows for the latest evaluation run.")
    else:
        metric_table = slices.copy()
        metric_table["f1_95_ci"] = [
            format_ci(row["f1"], row["ci_low_f1"], row["ci_high_f1"])
            for _, row in metric_table.iterrows()
        ]
        metric_table["precision"] = metric_table["precision"].map(
            lambda value: f"{safe_float(value):.3f}"
        )
        metric_table["recall"] = metric_table["recall"].map(
            lambda value: f"{safe_float(value):.3f}"
        )
        metric_table["pr_auc"] = metric_table["pr_auc"].map(
            lambda value: f"{safe_float(value):.3f}"
        )
        metric_table["support"] = metric_table["support"].map(format_count)
        metric_table["evaluated_at"] = metric_table["evaluated_at"].map(
            format_timestamp
        )
        st.caption(
            f"Latest eval run: `{slices.iloc[0]['eval_run_id']}`. CI is a "
            "document-level bootstrap interval over the held-out test split."
        )
        st.dataframe(
            metric_table[
                [
                    "model_name",
                    "slice_type",
                    "slice_value",
                    "entity_type",
                    "support",
                    "f1_95_ci",
                    "precision",
                    "recall",
                    "pr_auc",
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        import plotly.graph_objects as go

        figure = go.Figure()
        for model_name, part in slices.groupby("model_name"):
            part = part.sort_values("entity_type")
            upper = (part["ci_high_f1"] - part["f1"]).clip(lower=0)
            lower = (part["f1"] - part["ci_low_f1"]).clip(lower=0)
            figure.add_trace(
                go.Bar(
                    name=str(model_name),
                    x=part["entity_type"],
                    y=part["f1"],
                    error_y={
                        "type": "data",
                        "symmetric": False,
                        "array": upper.tolist(),
                        "arrayminus": lower.tolist(),
                    },
                    hovertemplate=(
                        "%{x}<br>F1 %{y:.3f}<extra>%{fullData.name}</extra>"
                    ),
                )
            )
        figure.update_layout(
            barmode="group",
            height=420,
            yaxis_title="F1 (error bars = 95% CI)",
            xaxis_title="entity type",
            legend_title="detector",
            margin={"t": 30, "b": 80},
        )
        st.plotly_chart(figure, width="stretch")

    st.subheader("Confusion matrix")
    confusion = query(
        ML_CONFUSION_SQL,
        {"row_limit": 2000},
        cache_key,
        columns=[
            "model_name",
            "slice_type",
            "slice_value",
            "entity_type",
            "tp",
            "fp",
            "fn",
            "tn",
        ],
    )
    if confusion.empty:
        st.info("No confusion-matrix rows for the latest evaluation run.")
    else:
        import plotly.graph_objects as go

        control_columns = st.columns(3)
        models = sorted(confusion["model_name"].dropna().unique().tolist())
        selected_model = control_columns[0].selectbox(
            "Detector", models, key="confusion_model"
        )
        subset = confusion[confusion["model_name"] == selected_model]
        slice_pairs = (
            subset[["slice_type", "slice_value"]]
            .drop_duplicates()
            .astype(str)
            .agg(" = ".join, axis=1)
            .tolist()
        )
        selected_pair = control_columns[1].selectbox(
            "Slice", slice_pairs, key="confusion_slice"
        )
        slice_type, _, slice_value = selected_pair.partition(" = ")
        subset = subset[
            (subset["slice_type"].astype(str) == slice_type)
            & (subset["slice_value"].astype(str) == slice_value)
        ]
        entities = sorted(subset["entity_type"].astype(str).unique().tolist())
        selected_entity = control_columns[2].selectbox(
            "Entity", entities, key="confusion_entity"
        )
        row = subset[subset["entity_type"].astype(str) == selected_entity]
        if row.empty:
            st.info("No confusion row for this selection.")
        else:
            record = row.iloc[0]
            matrix = [
                [safe_int(record["tp"]), safe_int(record["fn"])],
                [safe_int(record["fp"]), safe_int(record["tn"])],
            ]
            labels = [format_count(value) for value in (
                safe_int(record["tp"]),
                safe_int(record["fn"]),
                safe_int(record["fp"]),
                safe_int(record["tn"]),
            )]
            heatmap = go.Heatmap(
                z=matrix,
                x=["predicted positive", "predicted negative"],
                y=["actual positive", "actual negative"],
                text=[labels[0:2], labels[2:4]],
                texttemplate="%{text}",
                colorscale="Blues",
                showscale=False,
            )
            figure = go.Figure(data=heatmap)
            figure.update_layout(
                height=340,
                title=f"{selected_model} | {selected_entity} | {selected_pair}",
                margin={"t": 50, "b": 40},
            )
            st.plotly_chart(figure, width="stretch")
            st.caption(
                "Counts come from `marts.fct_ml_confusion`. TP/FP/FN/TN are "
                "span-level matches on the held-out test split."
            )

    st.subheader("Operational cost")
    operational = query(
        ML_OPERATIONAL_SQL,
        {},
        cache_key,
        columns=[
            "eval_run_id",
            "model_name",
            "median_latency_ms",
            "p95_latency_ms",
            "docs_per_sec",
            "peak_ram_mb",
            "artifact_size_mb",
            "docs_evaluated",
            "evaluated_at",
        ],
    )
    if operational.empty:
        st.info("No latency or throughput rows for the latest evaluation run.")
    else:
        display = operational.copy()
        display["median_latency_ms"] = display["median_latency_ms"].map(
            lambda value: f"{safe_float(value):.1f} ms"
        )
        display["p95_latency_ms"] = display["p95_latency_ms"].map(
            lambda value: f"{safe_float(value):.1f} ms"
        )
        display["docs_per_sec"] = display["docs_per_sec"].map(
            lambda value: f"{safe_float(value):.1f}"
        )
        display["peak_ram_mb"] = display["peak_ram_mb"].map(
            lambda value: f"{safe_float(value):.1f} MB"
        )
        display["artifact_size_mb"] = display["artifact_size_mb"].map(
            lambda value: f"{safe_float(value):.1f} MB"
        )
        display["docs_evaluated"] = display["docs_evaluated"].map(format_count)
        display["evaluated_at"] = display["evaluated_at"].map(format_timestamp)
        st.dataframe(display, width="stretch", hide_index=True)


def render_traceability(cache_key: str) -> None:
    """Tab 3: side-by-side extraction and rendered source page."""
    if st is None:
        return
    st.subheader("Unstructured extraction traceability")
    controls = st.columns([1, 1, 3])
    file_type = controls[0].selectbox(
        "File type", [ALL_OPTION, "pdf", "png"], key="trace_file_type"
    )
    min_confidence = controls[1].slider(
        "Minimum OCR confidence",
        min_value=0.0,
        max_value=1.0,
        value=0.0,
        step=0.05,
        key="trace_min_confidence",
    )
    search = controls[2].text_input(
        "Search extracted value, label or URI",
        placeholder="e.g. SSN, invoice, local://",
        key="trace_search",
    )

    params: dict[str, Any] = {
        "file_type": optional_filter(file_type),
        "min_confidence": None if min_confidence <= 0.0 else min_confidence,
        "search_like": like_filter(search),
        "row_limit": 200,
    }
    extractions = query(
        EXTRACTION_LIST_SQL,
        params,
        cache_key,
        columns=[
            "extraction_key",
            "document_key",
            "source_file_uri",
            "file_type",
            "page_number",
            "ocr_confidence",
            "extracted_value",
            "raw_entity_label",
            "privacy_tier",
            "has_high_risk_pii",
            "canonical_entity_label",
        ],
    )
    if extractions.empty:
        st.info(
            "No unstructured extractions match. Run the OCR stage:\n\n"
            "```bash\npython -m ingestion.generate_unstructured_docs "
            "--batch-id local_dev_001 --count 1000\n"
            "python -m ingestion.ocr_extract_worker --batch-id local_dev_001\n"
            "```"
        )
        return

    st.caption(
        f"{format_count(len(extractions))} extractions (max 200). Sorted by "
        "lowest OCR confidence first so uncertain rows are easy to find."
    )
    labels = {
        str(row["extraction_key"]): (
            f"{str(row['extracted_value'])[:48]} | {row['raw_entity_label']} | "
            f"page {row['page_number']} | conf {format_confidence(row['ocr_confidence'])}"
        )
        for _, row in extractions.iterrows()
    }
    selected_key = st.selectbox(
        "Select an extraction",
        list(labels.keys()),
        format_func=lambda key: labels.get(key, key),
        key="trace_extraction_key",
    )

    detail = query(
        EXTRACTION_DETAIL_SQL,
        (selected_key,),
        cache_key,
        columns=[
            "extraction_key",
            "document_key",
            "source_file_uri",
            "file_type",
            "page_number",
            "bbox_json",
            "ocr_confidence",
            "extracted_value",
            "raw_entity_label",
            "privacy_tier",
            "has_high_risk_pii",
            "needs_ocr_verification",
            "extracted_text",
            "language",
            "domain",
            "batch_id",
            "canonical_entity_label",
            "entity_description",
        ],
    )
    if detail.empty:
        st.warning("That extraction is no longer present. Press Refresh data.")
        return
    record = detail.iloc[0]
    bbox = parse_bbox(record.get("bbox_json"))

    left, right = st.columns(2)
    with left:
        st.markdown("**Extracted field**")
        st.metric(
            "OCR confidence",
            format_confidence(record.get("ocr_confidence")),
            delta="needs verification"
            if bool(record.get("needs_ocr_verification"))
            else "within policy",
            delta_color="inverse" if record.get("needs_ocr_verification") else "normal",
        )
        st.write(
            {
                "extracted_value": record.get("extracted_value"),
                "canonical_label": record.get("canonical_entity_label"),
                "raw_label": record.get("raw_entity_label"),
                "privacy_tier": record.get("privacy_tier"),
                "page_number": safe_int(record.get("page_number"), 0),
                "file_type": record.get("file_type"),
                "batch_id": record.get("batch_id"),
                "document_key": record.get("document_key"),
                "high_risk": bool(record.get("has_high_risk_pii")),
            }
        )
        st.caption(
            "bbox coordinates are `[x1, y1, x2, y2]` in 200 DPI page pixels."
        )
        st.code(record.get("source_file_uri") or "missing URI", language="text")
        if bbox is not None:
            st.code(json.dumps(bbox), language="json")
        with st.expander("Page OCR text"):
            st.text(str(record.get("extracted_text") or "")[:8000])
    with right:
        st.markdown("**Source page with bounding box**")
        render_source_page(
            record.get("source_file_uri"),
            safe_int(record.get("page_number"), 1),
            bbox,
            data_directory(),
            file_type=str(record.get("file_type") or ""),
        )


def render_source_page(
    source_file_uri: Any,
    page_number: int,
    bbox: Sequence[float] | None,
    data_dir: str | Path,
    file_type: str = "",
) -> None:
    """Render a PDF page or PNG with the bbox highlighted in red."""
    if st is None:
        return
    uri = str(source_file_uri or "")
    path = resolve_local_uri(uri, data_dir)
    if path is None:
        st.info(
            f"URI `{uri}` is not a `local://` path. Fetch the object from your "
            "bucket/stage to preview it. The extracted row and bbox are still "
            "shown on the left."
        )
        return
    if not path.exists():
        st.warning(
            f"Source file not found on disk: `{path}`\n\n"
            f"URI: `{uri}` | bbox: `{list(bbox) if bbox else 'n/a'}`\n\n"
            "Mount `PII_DATA_DIR` into the app container to enable previews."
        )
        return

    try:
        from PIL import Image, ImageDraw
    except ModuleNotFoundError:
        st.warning("Pillow is not installed; cannot draw the bounding box.")
        return

    suffix = path.suffix.lower()
    image = None
    draw_bbox = bbox
    try:
        if suffix == ".pdf" or file_type.lower() == "pdf":
            import fitz  # PyMuPDF

            with fitz.open(path) as document:
                page_index = max(page_number - 1, 0)
                if page_index >= document.page_count:
                    st.warning(
                        f"Page {page_number} is outside the PDF "
                        f"({document.page_count} pages): `{path}`"
                    )
                    return
                page = document.load_page(page_index)
                pixmap = page.get_pixmap(
                    matrix=fitz.Matrix(RENDER_ZOOM, RENDER_ZOOM), alpha=False
                )
                image = Image.frombytes(
                    "RGB", (pixmap.width, pixmap.height), pixmap.samples
                )
            if bbox is not None:
                draw_bbox = scale_bbox(bbox, PAGE_DPI, RENDER_ZOOM)
        else:
            image = Image.open(path).convert("RGB")
    except Exception as exc:  # noqa: BLE001 - corrupt files are user data
        st.warning(f"Could not render `{path}`: {redact_secrets(exc)}")
        return

    if image is None:
        st.warning(f"Unsupported preview type for `{path}`.")
        return

    if draw_bbox is not None:
        drawer = ImageDraw.Draw(image)
        drawer.rectangle(
            [draw_bbox[0], draw_bbox[1], draw_bbox[2], draw_bbox[3]],
            outline="red",
            width=3,
        )

    st.image(
        image,
        caption=(
            f"{uri} | page {page_number} | bbox "
            f"{list(bbox) if bbox else 'n/a'} (200 DPI page pixels)"
        ),
        width="stretch",
    )
    if suffix != ".pdf" and file_type.lower() != "pdf":
        st.caption("PNG pixels equal the 200 DPI page coordinate space.")


def _queue_action(task_id: str, status: str, reviewer: str, note: str) -> None:
    if st is None:
        return
    success, message = update_review_status(task_id, status, reviewer, note or None)
    if success:
        st.success(message)
        st.rerun()
    else:
        st.error(message)


def render_agent_actions(cache_key: str) -> None:
    """Tab 4: agent audit log and human-in-the-loop review queue."""
    if st is None:
        return
    st.subheader("Agent action history")
    filter_columns = st.columns(2)
    decisions = query(
        ACTION_DECISION_OPTIONS_SQL, {}, cache_key, columns=["decision"]
    )
    outcomes = query(
        ACTION_OUTCOME_OPTIONS_SQL, {}, cache_key, columns=["outcome"]
    )

    def options(frame: Any, column: str) -> list[str]:
        if frame.empty:
            return [ALL_OPTION]
        return [ALL_OPTION] + [str(value) for value in frame[column].tolist()]

    decision = filter_columns[0].selectbox(
        "Decision", options(decisions, "decision"), key="action_decision"
    )
    outcome = filter_columns[1].selectbox(
        "Outcome", options(outcomes, "outcome"), key="action_outcome"
    )
    params = {
        "decision": optional_filter(decision),
        "outcome": optional_filter(outcome),
        "row_limit": 500,
    }

    counts = query(
        ACTION_COUNTS_SQL,
        params,
        cache_key,
        columns=["decision", "outcome", "actions"],
    )
    if counts.empty:
        st.info(
            "No agent actions recorded. The agent is quality-gated: it only "
            "runs after dbt tests PASS.\n\n"
            "```bash\npython -m agent.triage_agent --max-actions 40\n```"
        )
    else:
        st.dataframe(counts, width="stretch", hide_index=True)

    actions = query(
        ACTION_LOG_SQL,
        params,
        cache_key,
        columns=[
            "executed_at",
            "action_id",
            "task_id",
            "document_key",
            "decision",
            "action_taken",
            "outcome",
            "otel_trace_id",
            "source_file_uri",
            "batch_id",
            "evidence_json",
        ],
    )
    if not actions.empty:
        display = actions.drop(columns=["evidence_json"]).copy()
        display["executed_at"] = display["executed_at"].map(format_timestamp)
        st.dataframe(display, width="stretch", hide_index=True)

        st.markdown("**Evidence for recent actions**")
        for _, row in actions.head(25).iterrows():
            title = (
                f"{format_timestamp(row.get('executed_at'))} | "
                f"{row.get('decision')} | {row.get('outcome')} | "
                f"{str(row.get('document_key'))[:16]}"
            )
            with st.expander(title):
                st.json(row.get("evidence_json"))
                st.caption(
                    f"action_id `{row.get('action_id')}` | trace "
                    f"`{row.get('otel_trace_id')}`"
                )

    st.divider()
    st.subheader("Human review queue (OPEN)")
    st.caption(
        "SLA: HIGH priority within 1 business day, everything else within 2 "
        "business days. Approving or dismissing here updates only "
        "`status`, `reviewed_by`, `reviewed_at` and `review_note` through the "
        "least-privilege `reviewer` role."
    )
    st.info(
        "Safety: agent actions are confined to the local review queue. External "
        "side effects are sandbox-only and opt-in via `SANDBOX_GITHUB_REPO`; "
        "this app never performs external writes."
    )

    queue_counts = query(
        QUEUE_STATUS_COUNTS_SQL, {}, cache_key, columns=["status", "tasks"]
    )
    if not queue_counts.empty:
        st.dataframe(queue_counts, width="stretch", hide_index=True)

    reviewer = st.text_input(
        "Reviewer name", value="streamlit_reviewer", key="reviewer_name"
    )
    queue = query(
        OPEN_REVIEW_QUEUE_SQL,
        {},
        cache_key,
        columns=[
            "task_id",
            "task_type",
            "document_key",
            "source_file_uri",
            "batch_id",
            "priority",
            "status",
            "assigned_role",
            "evidence_json",
            "created_by",
            "created_at",
        ],
    )
    if queue.empty:
        st.success("No OPEN review tasks. Nothing needs human sign-off.")
        return

    for _, task in queue.iterrows():
        task_id = str(task.get("task_id"))
        heading = (
            f"{task.get('priority')} | {task.get('task_type')} | "
            f"{str(task.get('document_key'))[:24]} | "
            f"{format_timestamp(task.get('created_at'))}"
        )
        with st.expander(heading):
            st.write(
                {
                    "batch_id": task.get("batch_id"),
                    "assigned_role": task.get("assigned_role"),
                    "created_by": task.get("created_by"),
                    "source_file_uri": task.get("source_file_uri"),
                }
            )
            st.json(task.get("evidence_json"))
            note = st.text_input(
                "Review note", key=f"note_{task_id}", placeholder="optional"
            )
            buttons = st.columns(3)
            if buttons[0].button("Approve", key=f"approve_{task_id}"):
                _queue_action(task_id, "APPROVED", reviewer, note)
            if buttons[1].button("Dismiss", key=f"dismiss_{task_id}"):
                _queue_action(task_id, "DISMISSED", reviewer, note)
            if buttons[2].button("Escalate", key=f"escalate_{task_id}"):
                _queue_action(task_id, "ESCALATED", reviewer, note)


def main() -> None:
    """Entry point; Streamlit executes this module as ``__main__``."""
    if st is None:
        raise SystemExit(
            "streamlit is not installed. Install requirements-app.txt to run "
            "the reviewer app."
        )
    st.set_page_config(
        layout="wide",
        page_title=APP_TITLE,
        initial_sidebar_state="expanded",
    )
    cache_key = load_cache_key()
    render_sidebar(cache_key)
    render_header(cache_key)
    tab_health, tab_risk, tab_trace, tab_agent = st.tabs(list(TAB_LABELS))
    with tab_health:
        render_pipeline_health(cache_key)
    with tab_risk:
        render_corpus_risk(cache_key)
    with tab_trace:
        render_traceability(cache_key)
    with tab_agent:
        render_agent_actions(cache_key)


if __name__ == "__main__":
    main()
