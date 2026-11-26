"""Shared configuration for the local PII hygiene platform.

All components (ingestion, dbt wrapper, ML eval, agent, app) read the same
environment contract, so the same image can run in Docker Compose, Kubernetes
or a GCP Cloud Run Job without code changes.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


def project_root() -> Path:
    """Repository root (works from any working directory)."""
    return Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    batch_id: str
    data_dir: Path
    db_host: str
    db_port: int
    db_name: str
    db_user: str
    db_password: str
    quarantine_threshold: float
    eval_max_docs: int
    eval_bootstrap: int
    agent_max_actions: int
    otel_endpoint: str
    openlineage_url: str
    sandbox_github_repo: str
    sandbox_github_token: str

    @property
    def dsn(self) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={self.db_user} password={self.db_password}"
        )

    def dsn_for(self, user: str, password: str) -> str:
        return (
            f"host={self.db_host} port={self.db_port} dbname={self.db_name} "
            f"user={user} password={password}"
        )

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def staged_dir(self) -> Path:
        return self.data_dir / "staged"

    @property
    def unstructured_dir(self) -> Path:
        return self.data_dir / "unstructured"

    @property
    def observability_dir(self) -> Path:
        return self.data_dir / "observability"


def get_settings() -> Settings:
    return Settings(
        batch_id=_env("PII_BATCH_ID", "local_dev_001"),
        data_dir=Path(_env("PII_DATA_DIR", str(project_root() / "data"))),
        db_host=_env("PII_DB_HOST", "127.0.0.1"),
        db_port=int(_env("PII_DB_PORT", "5433")),
        db_name=_env("PII_DB_NAME", "pii_lab"),
        db_user=_env("PII_DB_USER", "pii_admin"),
        db_password=_env("PII_DB_PASSWORD", "pii_admin_pw"),
        quarantine_threshold=float(_env("PII_QUARANTINE_THRESHOLD", "0.05")),
        eval_max_docs=int(_env("PII_EVAL_MAX_DOCS", "4000")),
        eval_bootstrap=int(_env("PII_EVAL_BOOTSTRAP", "1000")),
        agent_max_actions=int(_env("PII_AGENT_MAX_ACTIONS", "40")),
        otel_endpoint=_env("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
        openlineage_url=_env("OPENLINEAGE_URL", ""),
        sandbox_github_repo=_env("SANDBOX_GITHUB_REPO", ""),
        sandbox_github_token=_env("SANDBOX_GITHUB_TOKEN", ""),
    )
