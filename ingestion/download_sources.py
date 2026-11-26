"""Explicit acquisition CLI for the four optional Hugging Face corpora."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from pathlib import Path

from common.config import get_settings
from common.lineage import emit_event
from common.telemetry import traced
from ingestion.fixtures import write_offline_source_fixtures

DOWNLOADS: tuple[tuple[str, str, str], ...] = (
    ("ai4privacy/pii-masking-openpii-1.5m", "openpii_train.jsonl", "train.jsonl"),
    ("nvidia/Nemotron-PII", "nemotron_train.parquet", "train.parquet"),
    ("mks-logic/SPY", "spy_medical.jsonl", "medical.jsonl"),
    ("mks-logic/SPY", "spy_legal.jsonl", "legal.jsonl"),
    ("Protegrity/pii-scope", "pii_scope/pii_scope.csv", "pii_scope.csv"),
)


def _find_download(cache_dir: Path, preferred: str) -> Path | None:
    candidates = sorted(cache_dir.rglob(Path(preferred).name))
    if candidates:
        return candidates[0]
    suffix = Path(preferred).suffix
    fallback = sorted(cache_dir.rglob(f"*{suffix}"))
    return fallback[0] if fallback else None


def download_repository(repo_id: str, destination: Path, preferred: str) -> bool:
    """Download one repository with ``hf`` and copy a matching data file."""
    if shutil.which("hf") is None:
        print(f"hf CLI unavailable; skipped {repo_id}")
        return False
    cache_dir = destination.parent / f".download-{destination.stem}"
    result = subprocess.run(
        ["hf", "download", repo_id, "--repo-type", "dataset", "--local-dir", str(cache_dir)],
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"Unable to download {repo_id}: {result.stderr.strip() or 'access denied'}")
        return False
    source = _find_download(cache_dir, preferred)
    if source is None:
        print(f"Downloaded {repo_id}, but found no {Path(preferred).suffix} data file")
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    return True


def download_sources(
    data_dir: Path,
    include_validation: bool = False,
    offline_fixtures: bool = False,
) -> dict[str, bool]:
    """Acquire public sources, or explicitly generate tiny offline fixtures."""
    raw_dir = data_dir / "raw"
    if offline_fixtures:
        paths = write_offline_source_fixtures(raw_dir)
        print("Wrote offline fixtures: " + ", ".join(str(path) for path in paths.values()))
        return {name: True for name in paths}

    outcomes: dict[str, bool] = {}
    for repo_id, filename, preferred in DOWNLOADS:
        outcomes[filename] = download_repository(repo_id, raw_dir / filename, preferred)
    if include_validation:
        outcomes["openpii_validation.jsonl"] = download_repository(
            "ai4privacy/pii-masking-openpii-1.5m",
            raw_dir / "openpii_validation.jsonl",
            "validation.jsonl",
        )
    print(
        "Protegrity/pii-scope is gated. If access is unavailable, build_batches uses "
        "a clearly labelled synthetic CSV stand-in; no file is fabricated under the real name."
    )
    return outcomes


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--include-validation", action="store_true")
    parser.add_argument("--offline-fixtures", action="store_true")
    args = parser.parse_args(argv)
    data_dir = args.data_dir or get_settings().data_dir
    emit_event("ingestion.download_sources", "START", outputs=["data.raw"])
    with traced("ingestion.download_sources"):
        outcomes = download_sources(data_dir, args.include_validation, args.offline_fixtures)
    emit_event(
        "ingestion.download_sources",
        "COMPLETE",
        outputs=["data.raw"],
        facets={"successful": sum(outcomes.values())},
    )
    return 0 if any(outcomes.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
