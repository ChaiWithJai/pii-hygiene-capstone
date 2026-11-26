"""Reproducible, group-aware evaluation of local PII detectors.

The module is split into pure, importable helpers (label canonicalisation,
regex detection, span matching, splits, bootstrap confidence intervals) and a
``main`` entry point that talks to Postgres. Importing this module never opens a
database connection and never loads a spaCy model, so unit tests stay fast and
offline.

Usage::

    python -m ml.evaluate_pii_detectors --eval-run-id 20261126T061500Z_ab12cd3
"""

from __future__ import annotations

import argparse
import hashlib
import re
import resource
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

# ---------------------------------------------------------------------------
# Canonical label taxonomy (aligned with the dbt ``dim_entity_type`` seed)
# ---------------------------------------------------------------------------

CANONICAL_LABELS: tuple[str, ...] = (
    "email",
    "phone",
    "ssn",
    "credit_card",
    "date",
    "ip_address",
    "url",
    "account_number",
    "given_name",
    "surname",
    "middle_name",
    "person_name",
    "address",
    "city",
    "state",
    "postal_code",
    "country",
    "organization",
    "location",
    "passport",
    "driver_license",
    "medical_record_number",
    "national_id",
    "username",
    "password",
    "date_of_birth",
    "age",
    "gender",
    "health_condition",
    "financial",
    "tax_id",
    "other",
)

_LABEL_ALIASES: dict[str, str] = {
    "email": "email",
    "email_address": "email",
    "emailaddress": "email",
    "emailaddresses": "email",
    "phone": "phone",
    "phone_number": "phone",
    "phonenumber": "phone",
    "phonenum": "phone",
    "telephone": "phone",
    "telephone_number": "phone",
    "telephonenum": "phone",
    "telephonenumber": "phone",
    "mobile": "phone",
    "mobile_number": "phone",
    "ssn": "ssn",
    "socialnum": "ssn",
    "social_security_number": "ssn",
    "socialsecuritynumber": "ssn",
    "social_security": "ssn",
    "us_ssn": "ssn",
    "ssn_number": "ssn",
    "credit_card": "credit_card",
    "creditcard": "credit_card",
    "credit_card_number": "credit_card",
    "card_number": "credit_card",
    "ccn": "credit_card",
    "payment_card": "credit_card",
    "date": "date",
    "dates": "date",
    "date_of_birth": "date_of_birth",
    "dob": "date_of_birth",
    "birthdate": "date_of_birth",
    "birth_date": "date_of_birth",
    "ip": "ip_address",
    "ip_address": "ip_address",
    "ipaddress": "ip_address",
    "ipv4": "ip_address",
    "ipv6": "ip_address",
    "url": "url",
    "uri": "url",
    "website": "url",
    "web_address": "url",
    "account": "account_number",
    "account_number": "account_number",
    "accountnum": "account_number",
    "bank_account": "account_number",
    "bank_account_number": "account_number",
    "iban": "account_number",
    "first_name": "given_name",
    "firstname": "given_name",
    "given_name": "given_name",
    "givenname": "given_name",
    "given": "given_name",
    "last_name": "surname",
    "lastname": "surname",
    "surname": "surname",
    "family_name": "surname",
    "familyname": "surname",
    "middle_name": "middle_name",
    "middlename": "middle_name",
    "name": "person_name",
    "full_name": "person_name",
    "fullname": "person_name",
    "person": "person_name",
    "person_name": "person_name",
    "human_name": "person_name",
    "address": "address",
    "street_address": "address",
    "home_address": "address",
    "postal_address": "address",
    "city": "city",
    "town": "city",
    "state": "state",
    "province": "state",
    "zip": "postal_code",
    "zipcode": "postal_code",
    "zip_code": "postal_code",
    "postal_code": "postal_code",
    "postcode": "postal_code",
    "country": "country",
    "organization": "organization",
    "organisation": "organization",
    "org": "organization",
    "company": "organization",
    "company_name": "organization",
    "employer": "organization",
    "location": "location",
    "loc": "location",
    "gpe": "location",
    "place": "location",
    "geo": "location",
    "passport": "passport",
    "passport_number": "passport",
    "driver_license": "driver_license",
    "drivers_license": "driver_license",
    "driving_licence": "driver_license",
    "license_number": "driver_license",
    "medical_record_number": "medical_record_number",
    "medical_record": "medical_record_number",
    "mrn": "medical_record_number",
    "national_id": "national_id",
    "nationalid": "national_id",
    "government_id": "national_id",
    "tax_id": "tax_id",
    "tin": "tax_id",
    "username": "username",
    "user_name": "username",
    "login": "username",
    "password": "password",
    "passwd": "password",
    "age": "age",
    "gender": "gender",
    "sex": "gender",
    "health": "health_condition",
    "health_condition": "health_condition",
    "medical_condition": "health_condition",
    "diagnosis": "health_condition",
    "medication": "health_condition",
    "financial": "financial",
    "salary": "financial",
    "income": "financial",
}

_PRIVACY_TIER_RANK: dict[str, int] = {
    "direct_identifier": 4,
    "sensitive_phi_financial": 3,
    "credential_secret": 2,
    "quasi_identifier": 1,
}

_DIRECT_IDENTIFIER_LABELS = {
    "email",
    "phone",
    "ssn",
    "credit_card",
    "account_number",
    "passport",
    "driver_license",
    "medical_record_number",
    "national_id",
    "tax_id",
    "username",
    "password",
    "address",
}
_PHI_LABELS = {"health_condition", "date_of_birth", "financial"}
_QUASI_LABELS = {
    "date",
    "age",
    "gender",
    "city",
    "state",
    "postal_code",
    "country",
    "organization",
    "location",
    "person_name",
    "given_name",
    "surname",
    "middle_name",
    "url",
    "ip_address",
}


def _slug(value: str) -> str:
    """Normalise a raw label into a lowercase underscore token."""
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_")


def canonicalize_label(raw: str | None) -> str:
    """Map a raw dataset/entity label to the canonical evaluation taxonomy.

    Unknown labels become ``"other"`` so they are counted in the audit but
    excluded from per-entity precision/recall/F1.
    """
    if raw is None:
        return "other"
    slug = _slug(str(raw))
    if not slug:
        return "other"
    if slug in _LABEL_ALIASES:
        return _LABEL_ALIASES[slug]
    if slug in CANONICAL_LABELS:
        return slug

    # Dataset-specific prefixes/suffixes (e.g. ``B-EMAIL``, ``email_address_1``).
    for prefix in ("b_", "i_", "s_", "e_", "label_", "pii_"):
        if slug.startswith(prefix):
            candidate = slug[len(prefix) :]
            if candidate in _LABEL_ALIASES:
                return _LABEL_ALIASES[candidate]
    for suffix in ("_value", "_text", "_1", "_2"):
        if slug.endswith(suffix):
            candidate = slug[: -len(suffix)]
            if candidate in _LABEL_ALIASES:
                return _LABEL_ALIASES[candidate]
    if slug.endswith("_number") and slug[:-7] in _LABEL_ALIASES:
        return _LABEL_ALIASES[slug[:-7]]
    return "other"


def infer_privacy_tier(entity: str, explicit: str | None = None) -> str:
    """Return a privacy tier, preferring the warehouse-provided value."""
    if explicit:
        slug = _slug(explicit)
        if slug in _PRIVACY_TIER_RANK:
            return slug
    if entity in _DIRECT_IDENTIFIER_LABELS:
        return "direct_identifier"
    if entity in _PHI_LABELS:
        return "sensitive_phi_financial"
    if entity in _QUASI_LABELS:
        return "quasi_identifier"
    return "quasi_identifier"


def dominant_privacy_tier(tiers: Iterable[str | None]) -> str:
    """Pick the most severe tier present in a document."""
    best = "quasi_identifier"
    best_rank = 0
    for tier in tiers:
        slug = _slug(tier or "")
        rank = _PRIVACY_TIER_RANK.get(slug, 0)
        if rank > best_rank:
            best, best_rank = slug, rank
    return best


# ---------------------------------------------------------------------------
# Spans and deterministic regex baseline
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Span:
    """A character-offset PII span (end-exclusive, Python indices)."""

    start: int
    end: int
    label: str
    value: str = ""
    privacy_tier: str = ""

    def overlaps(self, other: Span) -> bool:
        """True when two half-open character intervals overlap."""
        return self.start < other.end and other.start < self.end


_EMAIL = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_PHONE = re.compile(
    r"(?<!\d)(?:\+?1[\s\-.]?)?(?:\(\d{3}\)|\d{3})[\s\-.]?\d{3}[\s\-.]?\d{4}(?!\d)"
)
_SSN = re.compile(r"(?<!\d)\d{3}-\d{2}-\d{4}(?!\d)")
_CREDIT_CARD = re.compile(r"(?<!\d)(?:\d[ \-]?){13,19}(?!\d)")
_DATE = re.compile(
    r"\b(?:"
    r"\d{4}-\d{2}-\d{2}"
    r"|\d{1,2}/\d{1,2}/\d{2,4}"
    r"|\d{1,2}-\d{1,2}-\d{4}"
    r")\b"
)
_IPV4 = re.compile(
    r"(?<![\d.])(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}"
    r"(?:25[0-5]|2[0-4]\d|1?\d?\d)(?![\d.])"
)
_URL = re.compile(r"https?://[^\s<>\"']+")
_ACCOUNT = re.compile(
    r"\b(?:ACCT|ACCOUNT|IBAN)[\s:#-]*([A-Z]{0,2}\d[\dA-Z \-]{7,30})(?!\w)",
    re.IGNORECASE,
)

# Ordered by specificity: earlier patterns win when intervals overlap.
_REGEX_SPECS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("email", _EMAIL),
    ("url", _URL),
    ("ssn", _SSN),
    ("credit_card", _CREDIT_CARD),
    ("ip_address", _IPV4),
    ("phone", _PHONE),
    ("date", _DATE),
    ("account_number", _ACCOUNT),
)


def _luhn_valid(digits: str) -> bool:
    """Validate a candidate card number with the Luhn checksum."""
    if not digits.isdigit() or not 13 <= len(digits) <= 19:
        return False
    total = 0
    parity = len(digits) % 2
    for index, char in enumerate(digits):
        value = int(char)
        if index % 2 == parity:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def _dedupe_overlaps(spans: Iterable[Span]) -> list[Span]:
    """Keep the earliest, most specific non-overlapping spans."""
    ordered = sorted(spans, key=lambda span: (span.start, -(span.end - span.start)))
    kept: list[Span] = []
    for span in ordered:
        if span.start < 0 or span.end <= span.start:
            continue
        if any(span.overlaps(existing) for existing in kept):
            continue
        kept.append(span)
    return kept


def detect_regex(text: str) -> list[Span]:
    """Run deterministic regex recognizers over a document.

    Returns canonical, non-overlapping spans with end-exclusive offsets.
    """
    if not text:
        return []
    found: list[Span] = []
    for label, pattern in _REGEX_SPECS:
        for match in pattern.finditer(text):
            start, end = match.span()
            value = text[start:end]
            if label == "credit_card" and not _luhn_valid(re.sub(r"\D", "", value)):
                continue
            if label == "account_number" and match.group(1):
                start, end = match.span(1)
                value = text[start:end]
            found.append(
                Span(
                    start=start,
                    end=end,
                    label=label,
                    value=value,
                    privacy_tier=infer_privacy_tier(label),
                )
            )
    return _dedupe_overlaps(found)


# ---------------------------------------------------------------------------
# Contextual lexicon fallback ("contextual_lexicon_ner")
# ---------------------------------------------------------------------------

_CAPITALIZED = re.compile(r"\b[A-Z][a-zA-Z'\-]+(?:\s+[A-Z][a-zA-Z'\-]+){0,3}\b")
_NAME_CUES = ("name", "mr", "mrs", "ms", "dr", "patient", "signed", "attn")
_ORG_CUES = ("company", "inc", "corp", "organization", "organisation", "employer")
_ADDRESS_CUES = ("address", "street", "avenue", "road", "lane", "suite", "floor")
_LOCATION_CUES = ("in", "at", "from", "city of", "county", "state of")


def detect_contextual_lexicon(text: str) -> list[Span]:
    """Deterministic fallback NER based on capitalisation + keyword context."""
    if not text:
        return []
    found: list[Span] = []
    for match in _CAPITALIZED.finditer(text):
        prefix = text[max(0, match.start() - 32) : match.start()].lower()
        value = match.group(0)
        label = ""
        if any(cue in prefix.split() for cue in _NAME_CUES) or prefix.rstrip().endswith(
            "name:"
        ):
            label = "person_name"
        elif any(cue in prefix for cue in _ORG_CUES):
            label = "organization"
        elif any(cue in prefix for cue in _ADDRESS_CUES):
            label = "address"
        elif any(cue in prefix for cue in _LOCATION_CUES):
            label = "location"
        if label:
            found.append(
                Span(
                    start=match.start(),
                    end=match.end(),
                    label=label,
                    value=value,
                    privacy_tier=infer_privacy_tier(label),
                )
            )
    return _dedupe_overlaps(found)


# ---------------------------------------------------------------------------
# NER runtime
# ---------------------------------------------------------------------------


@dataclass
class NerRuntime:
    """A loaded NER detector plus metadata recorded in the eval output."""

    name: str
    nlp: Any | None = None
    artifact_size_mb: float = 0.0
    fallback_reason: str = ""

    @property
    def used_fallback(self) -> bool:
        return self.nlp is None


_SPACY_TO_CANONICAL = {
    "PERSON": "person_name",
    "PER": "person_name",
    "ORG": "organization",
    "LOC": "location",
    "GPE": "location",
}


def _model_size_mb(model_name: str) -> float:
    """Best-effort size of a spaCy model directory on disk."""
    try:  # pragma: no cover - depends on local install
        import spacy

        path = Path(spacy.util.get_package_path(model_name))  # type: ignore[attr-defined]
        total = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
        return round(total / (1024 * 1024), 4)
    except Exception:
        return 0.0


def load_ner_runtime(model_name: str = "xx_ent_wiki_sm") -> NerRuntime:
    """Load ``xx_ent_wiki_sm`` once; fall back to the lexicon detector.

    Never raises: a missing spaCy install or model simply selects the
    documented deterministic fallback.
    """
    try:  # pragma: no cover - import depends on local install
        import spacy

        nlp = spacy.load(model_name, disable=["parser", "lemmatizer"])
        size = _model_size_mb(model_name)
        return NerRuntime(name=model_name, nlp=nlp, artifact_size_mb=size)
    except Exception as error:  # noqa: BLE001 - fallback is intentional
        return NerRuntime(
            name="contextual_lexicon_ner",
            nlp=None,
            artifact_size_mb=0.0,
            fallback_reason=f"{type(error).__name__}: {error}",
        )


def _ner_spans_from_doc(doc: Any) -> list[Span]:
    """Convert a spaCy ``Doc`` into canonical spans."""
    spans: list[Span] = []
    for ent in getattr(doc, "ents", []):
        label = _SPACY_TO_CANONICAL.get(ent.label_)
        if not label:
            continue
        spans.append(
            Span(
                start=int(ent.start_char),
                end=int(ent.end_char),
                label=label,
                value=ent.text,
                privacy_tier=infer_privacy_tier(label),
            )
        )
    return spans


def run_detector(
    runtime: NerRuntime,
    texts: Sequence[str],
    *,
    batch_size: int = 64,
) -> tuple[list[list[Span]], list[float]]:
    """Run one detector over documents, merging structured regex patterns.

    The NER detector intentionally merges the deterministic structured
    recognizers (email/phone/SSN/card/...) because the multilingual model only
    covers names/organisations/locations. Latency is measured per document.
    """
    predictions: list[list[Span]] = []
    latencies_ms: list[float] = []
    batch = max(1, int(batch_size))
    if runtime.nlp is not None:  # pragma: no cover - requires spaCy model
        for offset in range(0, len(texts), batch):
            chunk = list(texts[offset : offset + batch])
            started = time.perf_counter()
            docs = list(runtime.nlp.pipe(chunk, batch_size=batch, n_process=1))
            elapsed = (time.perf_counter() - started) * 1000
            per_doc = elapsed / max(1, len(chunk))
            for text, doc in zip(chunk, docs, strict=False):
                merged = _dedupe_overlaps(
                    [*_ner_spans_from_doc(doc), *detect_regex(text)]
                )
                predictions.append(merged)
                latencies_ms.append(per_doc)
        return predictions, latencies_ms

    for text in texts:
        started = time.perf_counter()
        merged = _dedupe_overlaps(
            [*detect_contextual_lexicon(text), *detect_regex(text)]
        )
        predictions.append(merged)
        latencies_ms.append((time.perf_counter() - started) * 1000)
    return predictions, latencies_ms


def run_regex_detector(texts: Sequence[str]) -> tuple[list[list[Span]], list[float]]:
    """Time the regex baseline per document."""
    predictions: list[list[Span]] = []
    latencies_ms: list[float] = []
    for text in texts:
        started = time.perf_counter()
        predictions.append(detect_regex(text))
        latencies_ms.append((time.perf_counter() - started) * 1000)
    return predictions, latencies_ms


# ---------------------------------------------------------------------------
# Span matching
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchCounts:
    """Precision/recall counters for one entity type."""

    tp: int = 0
    fp: int = 0
    fn: int = 0

    @property
    def precision(self) -> float:
        denominator = self.tp + self.fp
        return self.tp / denominator if denominator else 0.0

    @property
    def recall(self) -> float:
        denominator = self.tp + self.fn
        return self.tp / denominator if denominator else 0.0

    @property
    def f1(self) -> float:
        precision, recall = self.precision, self.recall
        return (
            2 * precision * recall / (precision + recall)
            if precision + recall
            else 0.0
        )


def greedy_match_spans(
    predicted: Sequence[Span],
    truth: Sequence[Span],
    *,
    label_sensitive: bool = True,
) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    """Greedy one-to-one exact character-offset span matching.

    Returns ``(matches, unmatched_prediction_indices, unmatched_truth_indices)``
    where each match is a ``(prediction_index, truth_index)`` pair. Candidates
    are processed in character order, so the result is deterministic.
    """
    used_truth: set[int] = set()
    matches: list[tuple[int, int]] = []
    unmatched_pred: list[int] = []
    pred_order = sorted(range(len(predicted)), key=lambda i: predicted[i].start)
    truth_order = sorted(range(len(truth)), key=lambda i: truth[i].start)
    for pred_index in pred_order:
        pred = predicted[pred_index]
        found = -1
        for truth_index in truth_order:
            if truth_index in used_truth:
                continue
            candidate = truth[truth_index]
            if label_sensitive and pred.label != candidate.label:
                continue
            if pred.start == candidate.start and pred.end == candidate.end:
                found = truth_index
                break
        if found >= 0:
            used_truth.add(found)
            matches.append((pred_index, found))
        else:
            unmatched_pred.append(pred_index)
    unmatched_truth = [i for i in truth_order if i not in used_truth]
    return matches, unmatched_pred, unmatched_truth


def count_matches(predicted: Sequence[Span], truth: Sequence[Span]) -> MatchCounts:
    """Count tp/fp/fn for one entity type (labels assumed equal)."""
    matches, unmatched_pred, unmatched_truth = greedy_match_spans(
        predicted, truth, label_sensitive=True
    )
    return MatchCounts(
        tp=len(matches),
        fp=len(unmatched_pred),
        fn=len(unmatched_truth),
    )


def match_by_entity(
    predicted: Sequence[Span],
    truth: Sequence[Span],
    *,
    exclude_other: bool = True,
) -> tuple[dict[str, MatchCounts], int]:
    """Match spans per canonical entity.

    Returns ``(counts_by_entity, excluded_other_count)``. Spans labelled
    ``other`` are counted but excluded from scoring.
    """
    pred_groups: dict[str, list[Span]] = defaultdict(list)
    truth_groups: dict[str, list[Span]] = defaultdict(list)
    other_count = 0
    for span in predicted:
        if span.label == "other":
            other_count += 1
            if exclude_other:
                continue
        pred_groups[span.label].append(span)
    for span in truth:
        if span.label == "other":
            other_count += 1
            if exclude_other:
                continue
        truth_groups[span.label].append(span)
    labels = set(pred_groups) | set(truth_groups)
    counts = {
        label: count_matches(pred_groups.get(label, []), truth_groups.get(label, []))
        for label in sorted(labels)
    }
    return counts, other_count


# ---------------------------------------------------------------------------
# Group-aware splits
# ---------------------------------------------------------------------------


def groups_for_documents(documents: Sequence[EvaluatedDoc]) -> list[str]:
    """Group key per document: template hash, falling back to document key."""
    return [
        doc.template_hash or doc.document_key
        for doc in documents
    ]


def grouped_split_indices(
    groups: Sequence[str],
    *,
    seed: int = 42,
) -> dict[str, list[int]]:
    """Create deterministic 70/15/15 group-aware splits.

    Uses scikit-learn's ``GroupShuffleSplit`` when available; a hash-ordered
    fallback keeps the pipeline runnable in minimal environments.
    """
    if not groups:
        return {"train": [], "validation": [], "test": []}
    unique = sorted(set(groups))
    if len(unique) >= 3:
        try:
            from sklearn.model_selection import GroupShuffleSplit

            splitter = GroupShuffleSplit(
                n_splits=1, test_size=0.30, random_state=seed
            )
            indices = np.arange(len(groups)).reshape(-1, 1)
            train_idx, rest_idx = next(
                splitter.split(indices, groups=np.asarray(groups))
            )
            second = GroupShuffleSplit(
                n_splits=1, test_size=0.5, random_state=seed
            )
            rest_groups = np.asarray(groups)[rest_idx]
            val_rel, test_rel = next(
                second.split(rest_idx.reshape(-1, 1), groups=rest_groups)
            )
            return {
                "train": [int(i) for i in train_idx],
                "validation": [int(rest_idx[i]) for i in val_rel],
                "test": [int(rest_idx[i]) for i in test_rel],
            }
        except (ImportError, ValueError):
            pass

    ordered = sorted(
        unique,
        key=lambda group: hashlib.sha256(
            f"{seed}|{group}".encode()
        ).hexdigest(),
    )
    total = len(groups)
    group_sizes = Counter(groups)
    group_split: dict[str, str] = {}
    cumulative = 0
    for group in ordered:
        fraction = cumulative / total if total else 0.0
        if fraction < 0.70:
            destination = "train"
        elif fraction < 0.85:
            destination = "validation"
        else:
            destination = "test"
        group_split[group] = destination
        cumulative += group_sizes[group]
    # Guarantee every populated split is used whenever there are enough groups.
    if len(unique) >= 2 and "test" not in group_split.values():
        group_split[ordered[-1]] = "test"
    if len(unique) >= 3 and "validation" not in group_split.values():
        group_split[ordered[-2]] = "validation"
    if len(unique) >= 2 and "train" not in group_split.values():
        group_split[ordered[0]] = "train"
    train: list[int] = []
    validation: list[int] = []
    test: list[int] = []
    buckets = {
        "train": train,
        "validation": validation,
        "test": test,
    }
    for index, group in enumerate(groups):
        buckets[group_split[group]].append(index)
    return {"train": train, "validation": validation, "test": test}


def assert_no_group_overlap(
    groups: Sequence[str], splits: Mapping[str, Sequence[int]]
) -> None:
    """Raise ``AssertionError`` if any group leaks across splits."""
    seen: dict[str, str] = {}
    for split_name, indices in splits.items():
        for index in indices:
            group = groups[index]
            previous = seen.get(group)
            if previous is not None and previous != split_name:
                raise AssertionError(
                    f"group {group!r} leaked between {previous} and {split_name}"
                )
            seen[group] = split_name


def split_documents(
    documents: Sequence[EvaluatedDoc], *, seed: int = 42
) -> dict[str, list[EvaluatedDoc]]:
    """Split documents by group and assert zero template leakage."""
    groups = groups_for_documents(documents)
    indices = grouped_split_indices(groups, seed=seed)
    assert_no_group_overlap(groups, indices)
    return {
        split: [documents[i] for i in indices[split]]
        for split in ("train", "validation", "test")
    }


# ---------------------------------------------------------------------------
# Metrics and bootstrap confidence intervals
# ---------------------------------------------------------------------------


def bootstrap_f1_ci(
    tp: Sequence[float] | np.ndarray,
    fp: Sequence[float] | np.ndarray,
    fn: Sequence[float] | np.ndarray,
    *,
    n_iterations: int = 1000,
    seed: int = 42,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """Document-level bootstrap 95% CI for F1.

    Resamples documents with replacement; deterministic for a fixed seed.
    """
    tp_arr = np.asarray(tp, dtype=float)
    fp_arr = np.asarray(fp, dtype=float)
    fn_arr = np.asarray(fn, dtype=float)
    size = len(tp_arr)
    if size == 0 or n_iterations <= 0:
        return 0.0, 0.0
    rng = np.random.default_rng(seed)
    draws = rng.integers(0, size, size=(int(n_iterations), size))
    numerator = 2.0 * tp_arr[draws].sum(axis=1)
    denominator = (
        2.0 * tp_arr[draws].sum(axis=1)
        + fp_arr[draws].sum(axis=1)
        + fn_arr[draws].sum(axis=1)
    )
    f1 = np.divide(
        numerator,
        denominator,
        out=np.zeros_like(numerator),
        where=denominator > 0,
    )
    alpha = (1.0 - confidence) / 2.0
    low, high = np.percentile(f1, [alpha * 100, (1 - alpha) * 100])
    return float(low), float(high)


def average_precision(y_true: Sequence[int], y_score: Sequence[float]) -> float:
    """Document-level average precision, graceful for single-class inputs."""
    truth = np.asarray(y_true, dtype=int)
    scores = np.asarray(y_score, dtype=float)
    if len(truth) == 0 or truth.min() == truth.max():
        return 0.0
    try:
        from sklearn.metrics import average_precision_score

        return float(average_precision_score(truth, scores))
    except ImportError:  # pragma: no cover - sklearn is a pinned dependency
        order = np.argsort(-scores, kind="stable")
        ranked = truth[order]
        hits = 0
        precision_sum = 0.0
        for position, value in enumerate(ranked, start=1):
            if value:
                hits += 1
                precision_sum += hits / position
        return float(precision_sum / hits) if hits else 0.0


# ---------------------------------------------------------------------------
# Missingness audit
# ---------------------------------------------------------------------------

HANDLING_RULES: dict[str, str] = {
    "raw_text": "drop",
    "language": "impute",
    "domain": "impute",
    "template_hash": "missingness_indicator",
    "text_char_length": "impute",
    "span_count": "kept",
    "entity_density": "impute",
    "ocr_confidence": "missingness_indicator",
    "source_file_uri": "missingness_indicator",
}

VALID_HANDLING = ("kept", "drop", "impute", "missingness_indicator")


def handling_for_column(column: str) -> str:
    """Documented handling rule for a feature column."""
    return HANDLING_RULES.get(column, "kept")


@dataclass(frozen=True)
class MissingnessRecord:
    """One row of the data-quality/missingness audit."""

    table_name: str
    column_name: str
    null_count: int
    total_count: int
    null_fraction: float
    handling: str


def missingness_record(
    table_name: str,
    column_name: str,
    values: Iterable[Any],
) -> MissingnessRecord:
    """Compute null frequency and the documented handling rule."""
    materialized = list(values)
    total = len(materialized)
    nulls = sum(1 for value in materialized if value is None)
    fraction = (nulls / total) if total else 0.0
    return MissingnessRecord(
        table_name=table_name,
        column_name=column_name,
        null_count=nulls,
        total_count=total,
        null_fraction=fraction,
        handling=handling_for_column(column_name),
    )


# ---------------------------------------------------------------------------
# Documents and database loading
# ---------------------------------------------------------------------------

EVAL_JOB_NAME = "ml_evaluate_pii_detectors"
MART_DOCUMENTS = "marts.fct_pii_documents"
MART_SPANS = "marts.fct_pii_spans"
MART_UNSTRUCTURED = "marts.fct_unstructured_extractions"
STAGING_DOCUMENTS = "staging.stg_pii_documents"
RAW_DOCUMENTS = "raw.pii_documents_raw"


@dataclass
class EvaluatedDoc:
    """A document with gold spans and slice attributes."""

    document_key: str
    raw_text: str
    language: str | None
    domain: str | None
    template_hash: str | None
    modality: str
    source_dataset: str | None
    batch_id: str | None
    spans: list[Span] = field(default_factory=list)
    text_char_length: int | None = None
    span_count: int | None = None
    entity_density: float | None = None
    dominant_tier: str = "quasi_identifier"


class EvaluationError(RuntimeError):
    """Actionable evaluation failure surfaced to the CLI user."""


def _require_relation(conn: Any, relation: str, hint: str) -> None:
    from common.db import table_exists

    schema, _, table = relation.partition(".")
    if not table_exists(conn, schema, table):
        raise EvaluationError(
            f"required relation {relation} does not exist. {hint}"
        )


def _relation_columns(conn: Any, relation: str) -> set[str]:
    schema, _, table = relation.partition(".")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _load_structured_documents(conn: Any, max_docs: int) -> list[EvaluatedDoc]:
    """Load validated documents, joining text/template from staging or raw."""
    from common.db import read_sql, table_exists

    doc_columns = _relation_columns(conn, MART_DOCUMENTS)
    if "document_key" not in doc_columns:
        raise EvaluationError(f"{MART_DOCUMENTS} is missing document_key")

    def doc_col(name: str) -> str:
        return f"d.{name}" if name in doc_columns else "NULL"

    join_clause = ""
    text_expr = "NULL"
    template_expr = "NULL"
    language_expr = doc_col("language")
    domain_expr = doc_col("domain")

    has_staging = table_exists(conn, "staging", "stg_pii_documents")
    if has_staging:
        stg_columns = _relation_columns(conn, STAGING_DOCUMENTS)
        if "raw_text" in stg_columns:
            join_clause = (
                f"LEFT JOIN {STAGING_DOCUMENTS} s ON s.document_key = d.document_key"
            )
            text_expr = "s.raw_text"
            if "template_hash" in stg_columns:
                template_expr = "s.template_hash"
            if "language" in stg_columns:
                language_expr = "COALESCE(d.language, s.language)"
            if "domain" in stg_columns:
                domain_expr = "COALESCE(d.domain, s.domain)"
    if text_expr == "NULL" and table_exists(conn, "raw", "pii_documents_raw"):
        raw_columns = _relation_columns(conn, RAW_DOCUMENTS)
        if "raw_text" in raw_columns:
            join_clause = (
                "LEFT JOIN (SELECT DISTINCT ON (document_key) document_key, "
                "raw_text, template_hash, language, domain "
                f"FROM {RAW_DOCUMENTS} "
                "ORDER BY document_key, ingested_at DESC) r "
                "ON r.document_key = d.document_key"
            )
            text_expr = "r.raw_text"
            template_expr = "r.template_hash"
            language_expr = "COALESCE(d.language, r.language)"
            domain_expr = "COALESCE(d.domain, r.domain)"

    sql = f"""
        SELECT
            d.document_key,
            {doc_col("source_dataset")} AS source_dataset,
            {language_expr} AS language,
            {domain_expr} AS domain,
            {template_expr} AS template_hash,
            {text_expr} AS raw_text,
            {doc_col("text_char_length")} AS text_char_length,
            {doc_col("span_count")} AS span_count,
            {doc_col("entity_density")} AS entity_density,
            {doc_col("batch_id")} AS batch_id
        FROM {MART_DOCUMENTS} d
        {join_clause}
        ORDER BY md5(d.document_key)
        LIMIT %s
    """
    rows = read_sql(conn, sql, (int(max_docs),))
    documents: list[EvaluatedDoc] = []
    for row in rows:
        (
            document_key,
            source_dataset,
            language,
            domain,
            template_hash,
            raw_text,
            text_char_length,
            span_count,
            entity_density,
            batch_id,
        ) = row
        if raw_text is None:
            continue
        documents.append(
            EvaluatedDoc(
                document_key=str(document_key),
                raw_text=str(raw_text),
                language=language,
                domain=domain,
                template_hash=template_hash,
                modality="clean_structured_text",
                source_dataset=source_dataset,
                batch_id=batch_id,
                text_char_length=(
                    int(text_char_length)
                    if text_char_length is not None
                    else len(str(raw_text))
                ),
                span_count=int(span_count) if span_count is not None else None,
                entity_density=_safe_float(entity_density),
            )
        )
    return documents


def _load_structured_spans(
    conn: Any, document_keys: Sequence[str]
) -> dict[str, list[Span]]:
    """Load gold spans for the sampled documents."""
    from common.db import read_sql

    if not document_keys:
        return {}
    columns = _relation_columns(conn, MART_SPANS)
    required = {"document_key", "raw_entity_label", "start_offset", "end_offset"}
    if not required.issubset(columns):
        missing = sorted(required - columns)
        raise EvaluationError(f"{MART_SPANS} is missing columns: {missing}")
    tier_expr = "privacy_tier" if "privacy_tier" in columns else "NULL"
    sql = f"""
        SELECT document_key, raw_entity_label, start_offset, end_offset,
               {tier_expr} AS privacy_tier
        FROM {MART_SPANS}
        WHERE document_key = ANY(%s)
    """
    rows = read_sql(conn, sql, (list(document_keys),))
    grouped: dict[str, list[Span]] = defaultdict(list)
    for document_key, raw_label, start, end, tier in rows:
        if start is None or end is None:
            continue
        label = canonicalize_label(raw_label)
        grouped[str(document_key)].append(
            Span(
                start=int(start),
                end=int(end),
                label=label,
                value="",
                privacy_tier=infer_privacy_tier(label, tier),
            )
        )
    return grouped


def _load_unstructured_documents(
    conn: Any, limit: int
) -> tuple[list[EvaluatedDoc], list[float]]:
    """Build page-level OCR documents from the unstructured marts."""
    from common.db import read_sql, table_exists

    if not table_exists(conn, "marts", "fct_unstructured_extractions"):
        return [], []
    columns = _relation_columns(conn, MART_UNSTRUCTURED)
    if not {"document_key", "extracted_text"}.issubset(columns):
        return [], []
    page_expr = "page_number" if "page_number" in columns else "1"
    value_expr = "extracted_value" if "extracted_value" in columns else "NULL"
    label_expr = "raw_entity_label" if "raw_entity_label" in columns else "NULL"
    tier_expr = "privacy_tier" if "privacy_tier" in columns else "NULL"
    confidence_expr = (
        "ocr_confidence" if "ocr_confidence" in columns else "NULL"
    )
    language_expr = "language" if "language" in columns else "NULL"
    domain_expr = "domain" if "domain" in columns else "NULL"
    source_expr = (
        "source_dataset_key" if "source_dataset_key" in columns else "NULL"
    )

    sql = f"""
        SELECT document_key, {page_expr} AS page_number,
               extracted_text, {value_expr} AS extracted_value,
               {label_expr} AS raw_entity_label,
               {tier_expr} AS privacy_tier,
               {confidence_expr} AS ocr_confidence,
               {language_expr} AS language,
               {domain_expr} AS domain,
               {source_expr} AS source_dataset
        FROM {MART_UNSTRUCTURED}
        ORDER BY md5(document_key || ':' || {page_expr}::text)
        LIMIT %s
    """
    rows = read_sql(conn, sql, (max(100, int(limit) * 8),))
    grouped: dict[tuple[str, int], dict[str, Any]] = {}
    confidences: list[float] = []
    for row in rows:
        (
            document_key,
            page_number,
            extracted_text,
            extracted_value,
            raw_entity_label,
            privacy_tier,
            ocr_confidence,
            language,
            domain,
            source_dataset,
        ) = row
        if extracted_text is None:
            continue
        confidence = _safe_float(ocr_confidence)
        if confidence is not None:
            confidences.append(confidence)
        key = (str(document_key), int(page_number or 1))
        entry = grouped.setdefault(
            key,
            {
                "text": str(extracted_text),
                "language": language,
                "domain": domain,
                "source_dataset": source_dataset,
                "spans": [],
            },
        )
        if extracted_value is None or raw_entity_label is None:
            continue
        value = str(extracted_value)
        start = entry["text"].find(value)
        if start < 0:
            continue
        label = canonicalize_label(raw_entity_label)
        entry["spans"].append(
            Span(
                start=start,
                end=start + len(value),
                label=label,
                value=value,
                privacy_tier=infer_privacy_tier(label, privacy_tier),
            )
        )

    documents: list[EvaluatedDoc] = []
    for (document_key, page_number), entry in list(grouped.items())[:limit]:
        spans = _dedupe_overlaps(entry["spans"])
        documents.append(
            EvaluatedDoc(
                document_key=f"{document_key}:page:{page_number}",
                raw_text=entry["text"],
                language=entry["language"],
                domain=entry["domain"],
                template_hash=None,
                modality="ocr_perturbed_pdf",
                source_dataset=entry["source_dataset"],
                batch_id=None,
                spans=spans,
                text_char_length=len(entry["text"]),
                span_count=len(spans),
                entity_density=_safe_float(
                    len(spans) / max(1, len(entry["text"]) / 1000)
                ),
            )
        )
    return documents, confidences


def build_documents(
    conn: Any,
    *,
    max_docs: int,
) -> tuple[list[EvaluatedDoc], list[float], Counter[str]]:
    """Load structured + unstructured evaluation documents."""
    if max_docs <= 0:
        raise EvaluationError("--max-docs must be a positive integer")
    checkpoint = max(1, min(int(max_docs), int(max_docs * 0.85)))
    documents = _load_structured_documents(conn, checkpoint)
    spans = _load_structured_spans(
        conn, [doc.document_key for doc in documents]
    )
    unknown = Counter()
    for document in documents:
        document.spans = spans.get(document.document_key, [])
        for span in document.spans:
            if span.label == "other":
                unknown[str(span.label)] += 1
        document.dominant_tier = dominant_privacy_tier(
            span.privacy_tier for span in document.spans
        )
        if document.span_count is None:
            document.span_count = len(document.spans)
        if document.entity_density is None:
            document.entity_density = len(document.spans) / max(
                1.0, len(document.raw_text) / 1000
            )

    remaining = max(0, int(max_docs) - len(documents))
    unstructured: list[EvaluatedDoc] = []
    confidences: list[float] = []
    if remaining:
        unstructured, confidences = _load_unstructured_documents(
            conn, remaining
        )
    for document in unstructured:
        document.dominant_tier = dominant_privacy_tier(
            span.privacy_tier for span in document.spans
        )
        for span in document.spans:
            if span.label == "other":
                unknown[str(span.label)] += 1
    return [*documents, *unstructured], confidences, unknown


# ---------------------------------------------------------------------------
# Metric row construction
# ---------------------------------------------------------------------------


@dataclass
class DocScore:
    """Per-document scoring result used by slice aggregation/bootstrap."""

    document_key: str
    slices: list[tuple[str, str]]
    counts: dict[str, MatchCounts]
    truth_has: set[str]
    pred_has: set[str]


def _language_slice(language: str | None) -> str:
    if not language:
        return "non_en_multilingual"
    return "en" if str(language).lower().startswith("en") else "non_en_multilingual"


def doc_slices(document: EvaluatedDoc) -> list[tuple[str, str]]:
    """Slice membership keys for a document."""
    return [
        ("overall", "all"),
        ("privacy_tier", document.dominant_tier),
        ("modality", document.modality),
        ("language", _language_slice(document.language)),
    ]


def score_documents(
    documents: Sequence[EvaluatedDoc],
    predictions: Sequence[Sequence[Span]],
) -> list[DocScore]:
    """Compute per-document, per-entity span matches."""
    scores: list[DocScore] = []
    for document, predicted in zip(documents, predictions, strict=False):
        counts, _ = match_by_entity(predicted, document.spans)
        truth_has = {span.label for span in document.spans if span.label != "other"}
        pred_has = {span.label for span in predicted if span.label != "other"}
        scores.append(
            DocScore(
                document_key=document.document_key,
                slices=doc_slices(document),
                counts=counts,
                truth_has=truth_has,
                pred_has=pred_has,
            )
        )
    return scores


def build_metric_rows(
    eval_run_id: str,
    model_name: str,
    scores: Sequence[DocScore],
    *,
    bootstrap_iterations: int,
    seed: int = 42,
) -> list[tuple[Any, ...]]:
    """Aggregate slice metrics with bootstrapped F1 confidence intervals."""
    grouped: dict[tuple[str, str], list[DocScore]] = defaultdict(list)
    for score in scores:
        for slice_key in score.slices:
            grouped[slice_key].append(score)

    rows: list[tuple[Any, ...]] = []
    for (slice_type, slice_value), members in sorted(grouped.items()):
        entities = sorted(
            {label for member in members for label in member.counts}
            | {label for member in members for label in member.truth_has}
            | {label for member in members for label in member.pred_has}
        )
        for entity in entities:
            tp_values: list[int] = []
            fp_values: list[int] = []
            fn_values: list[int] = []
            y_true: list[int] = []
            y_score: list[float] = []
            tn = 0
            for member in members:
                counts = member.counts.get(entity, MatchCounts())
                tp_values.append(counts.tp)
                fp_values.append(counts.fp)
                fn_values.append(counts.fn)
                truth_present = entity in member.truth_has
                pred_present = entity in member.pred_has
                y_true.append(1 if truth_present else 0)
                y_score.append(1.0 if pred_present else 0.0)
                if not truth_present and not pred_present:
                    tn += 1
            tp = int(sum(tp_values))
            fp = int(sum(fp_values))
            fn = int(sum(fn_values))
            if tp == 0 and fp == 0 and fn == 0:
                continue
            counts = MatchCounts(tp=tp, fp=fp, fn=fn)
            low, high = bootstrap_f1_ci(
                tp_values,
                fp_values,
                fn_values,
                n_iterations=bootstrap_iterations,
                seed=seed,
            )
            rows.append(
                (
                    eval_run_id,
                    model_name,
                    "test",
                    slice_type,
                    slice_value,
                    entity,
                    counts.precision,
                    counts.recall,
                    counts.f1,
                    average_precision(y_true, y_score),
                    tp + fn,
                    tp,
                    fp,
                    fn,
                    low,
                    high,
                )
            )
    return rows


def build_confusion_rows(
    eval_run_id: str,
    model_name: str,
    scores: Sequence[DocScore],
) -> list[tuple[Any, ...]]:
    """Document-level confusion rows per slice and entity."""
    grouped: dict[tuple[str, str], list[DocScore]] = defaultdict(list)
    for score in scores:
        for slice_key in score.slices:
            grouped[slice_key].append(score)
    rows: list[tuple[Any, ...]] = []
    for (slice_type, slice_value), members in sorted(grouped.items()):
        entities = sorted(
            {label for member in members for label in member.counts}
            | {label for member in members for label in member.truth_has}
            | {label for member in members for label in member.pred_has}
        )
        for entity in entities:
            tp = fp = fn = tn = 0
            for member in members:
                counts = member.counts.get(entity, MatchCounts())
                tp += counts.tp
                fp += counts.fp
                fn += counts.fn
                if (
                    entity not in member.truth_has
                    and entity not in member.pred_has
                ):
                    tn += 1
            if tp == 0 and fp == 0 and fn == 0:
                continue
            rows.append(
                (
                    eval_run_id,
                    model_name,
                    "test",
                    slice_type,
                    slice_value,
                    entity,
                    tp,
                    fp,
                    fn,
                    tn,
                )
            )
    return rows


def operational_metrics(
    eval_run_id: str,
    model_name: str,
    latencies_ms: Sequence[float],
    docs_evaluated: int,
    artifact_size_mb: float,
) -> tuple[Any, ...]:
    """Build one ``raw.ml_operational_raw`` row."""
    if latencies_ms:
        median = float(np.median(latencies_ms))
        p95 = float(np.percentile(latencies_ms, 95))
        elapsed_seconds = sum(latencies_ms) / 1000.0
        docs_per_sec = docs_evaluated / elapsed_seconds if elapsed_seconds else 0.0
    else:
        median = p95 = docs_per_sec = 0.0
    peak_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    return (
        eval_run_id,
        model_name,
        round(median, 4),
        round(p95, 4),
        round(docs_per_sec, 4),
        round(peak_mb, 4),
        round(float(artifact_size_mb), 4),
        int(docs_evaluated),
    )


# ---------------------------------------------------------------------------
# Database writers
# ---------------------------------------------------------------------------


def _replace_run_rows(
    conn: Any,
    table: str,
    eval_run_id: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> int:
    """Idempotently replace one run's rows in a single transaction."""
    from common.db import execute_values

    with conn:
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {table} WHERE eval_run_id = %s", (eval_run_id,)
            )
        if not rows:
            return 0
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s"
        execute_values(conn, sql, rows)
        return len(rows)


def write_audit_rows(
    conn: Any, eval_run_id: str, records: Sequence[MissingnessRecord]
) -> int:
    """Write the missingness audit, replacing this run's prior rows."""
    columns = (
        "eval_run_id",
        "table_name",
        "column_name",
        "null_count",
        "total_count",
        "null_fraction",
        "handling",
    )
    rows = [
        (
            eval_run_id,
            record.table_name,
            record.column_name,
            record.null_count,
            record.total_count,
            record.null_fraction,
            record.handling,
        )
        for record in records
    ]
    return _replace_run_rows(
        conn, "raw.ml_data_audit_raw", eval_run_id, columns, rows
    )


def write_slice_rows(
    conn: Any, eval_run_id: str, rows: Sequence[tuple[Any, ...]]
) -> int:
    """Write metric rows to ``raw.ml_eval_slices_raw``."""
    columns = (
        "eval_run_id",
        "model_name",
        "split",
        "slice_type",
        "slice_value",
        "entity_type",
        "precision",
        "recall",
        "f1",
        "pr_auc",
        "support",
        "tp",
        "fp",
        "fn",
        "ci_low_f1",
        "ci_high_f1",
    )
    return _replace_run_rows(
        conn, "raw.ml_eval_slices_raw", eval_run_id, columns, rows
    )


def write_confusion_rows(
    conn: Any, eval_run_id: str, rows: Sequence[tuple[Any, ...]]
) -> int:
    """Write confusion rows to ``raw.ml_confusion_raw``."""
    columns = (
        "eval_run_id",
        "model_name",
        "split",
        "slice_type",
        "slice_value",
        "entity_type",
        "tp",
        "fp",
        "fn",
        "tn",
    )
    return _replace_run_rows(
        conn, "raw.ml_confusion_raw", eval_run_id, columns, rows
    )


def write_operational_rows(
    conn: Any, eval_run_id: str, rows: Sequence[tuple[Any, ...]]
) -> int:
    """Write operational trade-off rows to ``raw.ml_operational_raw``."""
    columns = (
        "eval_run_id",
        "model_name",
        "median_latency_ms",
        "p95_latency_ms",
        "docs_per_sec",
        "peak_ram_mb",
        "artifact_size_mb",
        "docs_evaluated",
    )
    return _replace_run_rows(
        conn, "raw.ml_operational_raw", eval_run_id, columns, rows
    )


# ---------------------------------------------------------------------------
# CLI orchestration
# ---------------------------------------------------------------------------


def _audit_records(
    documents: Sequence[EvaluatedDoc],
    ocr_confidences: Sequence[float],
) -> list[MissingnessRecord]:
    """Build the audit for document and unstructured feature columns."""
    table = STAGING_DOCUMENTS
    records = [
        missingness_record(table, "raw_text", [doc.raw_text for doc in documents]),
        missingness_record(
            table, "language", [doc.language for doc in documents]
        ),
        missingness_record(table, "domain", [doc.domain for doc in documents]),
        missingness_record(
            table, "template_hash", [doc.template_hash for doc in documents]
        ),
        missingness_record(
            table,
            "text_char_length",
            [doc.text_char_length for doc in documents],
        ),
        missingness_record(
            table, "span_count", [doc.span_count for doc in documents]
        ),
        missingness_record(
            table, "entity_density", [doc.entity_density for doc in documents]
        ),
        missingness_record(
            MART_UNSTRUCTURED,
            "ocr_confidence",
            list(ocr_confidences),
        ),
    ]
    return records


def _sql_missingness_records(
    conn: Any,
    relation: str,
    columns: Sequence[str],
) -> list[MissingnessRecord]:
    """Count nulls directly in the warehouse for the named columns."""
    from common.db import table_exists

    schema, _, table = relation.partition(".")
    if not table_exists(conn, schema, table):
        return []
    available = _relation_columns(conn, relation)
    selected = [column for column in columns if column in available]
    if not selected:
        return []
    filters = ", ".join(
        f"COUNT(*) FILTER (WHERE {column} IS NULL)" for column in selected
    )
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*), {filters} FROM {relation}")
        row = cur.fetchone()
    total = int(row[0])
    records: list[MissingnessRecord] = []
    for index, column in enumerate(selected, start=1):
        nulls = int(row[index])
        records.append(
            MissingnessRecord(
                table_name=relation,
                column_name=column,
                null_count=nulls,
                total_count=total,
                null_fraction=(nulls / total) if total else 0.0,
                handling=handling_for_column(column),
            )
        )
    return records


def _collect_audit_records(
    conn: Any,
    documents: Sequence[EvaluatedDoc],
    ocr_confidences: Sequence[float],
) -> list[MissingnessRecord]:
    """Prefer warehouse-wide null counts; fall back to the sampled documents."""
    from common.db import table_exists

    columns = (
        "raw_text",
        "language",
        "domain",
        "template_hash",
        "text_char_length",
        "span_count",
        "entity_density",
    )
    records: list[MissingnessRecord] = []
    if table_exists(conn, "staging", "stg_pii_documents"):
        records.extend(
            _sql_missingness_records(conn, STAGING_DOCUMENTS, columns)
        )
    elif table_exists(conn, "raw", "pii_documents_raw"):
        records.extend(
            _sql_missingness_records(conn, RAW_DOCUMENTS, columns)
        )
    present = {record.column_name for record in records}
    if present != set(columns):
        fallback = _audit_records(documents, [])
        records.extend(
            record for record in fallback if record.column_name not in present
        )
    records.extend(
        _sql_missingness_records(
            conn, MART_UNSTRUCTURED, ("ocr_confidence",)
        )
    )
    if not any(record.column_name == "ocr_confidence" for record in records):
        records.append(
            missingness_record(
                MART_UNSTRUCTURED,
                "ocr_confidence",
                list(ocr_confidences),
            )
        )
    return records


def _print_headline(
    slice_rows: Sequence[tuple[Any, ...]],
    model_names: Sequence[str],
) -> None:
    print("\nHeadline test-split metrics (canonical entity level)")
    header = (
        f"{'model':<24}{'slice':<28}{'entity':<20}"
        f"{'F1':>7}{'95% CI':>18}{'P':>8}{'R':>8}{'AUC':>8}{'n':>7}"
    )
    print(header)
    print("-" * len(header))
    for row in slice_rows:
        (
            _run,
            model,
            _split,
            slice_type,
            slice_value,
            entity,
            precision,
            recall,
            f1,
            pr_auc,
            support,
            _tp,
            _fp,
            _fn,
            low,
            high,
        ) = row
        if slice_type != "overall":
            continue
        print(
            f"{model:<24}{slice_type + ':' + slice_value:<28}{entity:<20}"
            f"{f1:>7.3f}{f'[{low:.3f},{high:.3f}]':>18}"
            f"{precision:>8.3f}{recall:>8.3f}{pr_auc:>8.3f}{support:>7}"
        )
    for model in model_names:
        print(f"  model: {model}")


def _run_evaluation(args: argparse.Namespace) -> int:
    from common.config import get_settings
    from common.db import connect
    from common.lineage import emit_event
    from common.telemetry import new_trace_id, traced

    settings = get_settings()
    eval_run_id = str(args.eval_run_id)
    max_docs = int(args.max_docs)
    bootstrap_iterations = int(args.bootstrap)
    if bootstrap_iterations < 0:
        raise EvaluationError("--bootstrap must be >= 0")

    trace_id = new_trace_id()
    started = time.perf_counter()
    with traced(  # noqa: SIM117 - one trace wraps the DB connection lifecycle
        EVAL_JOB_NAME,
        trace_id=trace_id,
        attributes={"eval_run_id": eval_run_id, "max_docs": max_docs},
    ):
        with connect(settings) as conn:
            _require_relation(
                conn,
                MART_DOCUMENTS,
                "Run dbt (pipeline -> dbt_run) before evaluating.",
            )
            _require_relation(
                conn,
                MART_SPANS,
                "Run dbt (pipeline -> dbt_run) before evaluating.",
            )
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {MART_DOCUMENTS}")
                total_docs = int(cur.fetchone()[0])
                cur.execute(f"SELECT COUNT(*) FROM {MART_SPANS}")
                total_spans = int(cur.fetchone()[0])
            if total_docs == 0:
                raise EvaluationError(
                    f"{MART_DOCUMENTS} is empty. Load raw data, then run dbt "
                    "to build the marts before evaluating."
                )
            if total_spans == 0:
                raise EvaluationError(
                    f"{MART_SPANS} is empty. dbt span models must be rebuilt "
                    "before evaluating."
                )

            documents, ocr_confidences, unknown = build_documents(
                conn, max_docs=max_docs
            )
            if not documents:
                raise EvaluationError(
                    "No documents with raw_text were loaded. Check that "
                    f"{STAGING_DOCUMENTS} (or {RAW_DOCUMENTS}) is populated."
                )

            splits = split_documents(documents, seed=42)
            groups = groups_for_documents(documents)
            index_by_id = {id(doc): index for index, doc in enumerate(documents)}
            split_indices = {
                name: [index_by_id[id(doc)] for doc in docs]
                for name, docs in splits.items()
            }
            assert_no_group_overlap(groups, split_indices)
            train_groups = set(groups_for_documents(splits["train"]))
            test_groups = set(groups_for_documents(splits["test"]))
            val_groups = set(groups_for_documents(splits["validation"]))
            print(
                "Grouped split assertion passed: "
                f"train={len(splits['train'])} "
                f"validation={len(splits['validation'])} "
                f"test={len(splits['test'])} docs; "
                f"train_groups={len(train_groups)} "
                f"val_groups={len(val_groups)} "
                f"test_groups={len(test_groups)}; "
                "template/document group overlap = 0"
            )
            print(
                f"Loaded {total_docs} mart docs / {total_spans} mart spans; "
                f"evaluating on {len(documents)} sampled docs "
                f"(test split is the only scored split)."
            )
            if unknown:
                print(f"Excluded unknown/other gold spans: {dict(unknown)}")

            test_docs = splits["test"]
            test_texts = [doc.raw_text for doc in test_docs]

            audit_records = _collect_audit_records(
                conn, documents, ocr_confidences
            )
            audit_written = write_audit_rows(conn, eval_run_id, audit_records)
            print(f"Missingness audit rows: {audit_written}")

            regex_predictions, regex_latencies = run_regex_detector(test_texts)
            runtime = load_ner_runtime()
            if runtime.used_fallback:
                print(
                    "spaCy model unavailable; using documented fallback "
                    f"{runtime.name} ({runtime.fallback_reason})"
                )
            ner_predictions, ner_latencies = run_detector(
                runtime, test_texts
            )

            emit_event(
                EVAL_JOB_NAME,
                "START",
                inputs=[MART_DOCUMENTS, MART_SPANS],
                outputs=[
                    "raw.ml_data_audit_raw",
                    "raw.ml_eval_slices_raw",
                    "raw.ml_confusion_raw",
                    "raw.ml_operational_raw",
                ],
                run_id=trace_id,
                facets={"eval_run_id": eval_run_id},
            )

            all_slice_rows: list[tuple[Any, ...]] = []
            all_confusion_rows: list[tuple[Any, ...]] = []
            operational_rows: list[tuple[Any, ...]] = []
            model_names = ["regex_baseline", runtime.name]
            for model_name, predictions, latencies, artifact_size in (
                (
                    "regex_baseline",
                    regex_predictions,
                    regex_latencies,
                    0.0,
                ),
                (
                    runtime.name,
                    ner_predictions,
                    ner_latencies,
                    runtime.artifact_size_mb,
                ),
            ):
                scores = score_documents(test_docs, predictions)
                slice_rows = build_metric_rows(
                    eval_run_id,
                    model_name,
                    scores,
                    bootstrap_iterations=bootstrap_iterations,
                )
                confusion_rows = build_confusion_rows(
                    eval_run_id, model_name, scores
                )
                all_slice_rows.extend(slice_rows)
                all_confusion_rows.extend(confusion_rows)
                operational_rows.append(
                    operational_metrics(
                        eval_run_id,
                        model_name,
                        latencies,
                        len(test_docs),
                        artifact_size,
                    )
                )

            slice_written = write_slice_rows(
                conn, eval_run_id, all_slice_rows
            )
            confusion_written = write_confusion_rows(
                conn, eval_run_id, all_confusion_rows
            )
            operational_written = write_operational_rows(
                conn, eval_run_id, operational_rows
            )

            _print_headline(all_slice_rows, model_names)
            elapsed = time.perf_counter() - started
            print(
                f"\nWrote {slice_written} slice rows, "
                f"{confusion_written} confusion rows, "
                f"{operational_written} operational rows for "
                f"eval_run_id={eval_run_id} in {elapsed:.1f}s "
                f"(trace_id={trace_id})."
            )

            emit_event(
                EVAL_JOB_NAME,
                "COMPLETE",
                inputs=[MART_DOCUMENTS, MART_SPANS],
                outputs=[
                    "raw.ml_data_audit_raw",
                    "raw.ml_eval_slices_raw",
                    "raw.ml_confusion_raw",
                    "raw.ml_operational_raw",
                ],
                run_id=trace_id,
                facets={
                    "eval_run_id": eval_run_id,
                    "slice_rows": slice_written,
                    "docs_evaluated": len(test_docs),
                },
            )
    return 0


def build_parser() -> argparse.ArgumentParser:
    """CLI parser for the evaluation job."""
    parser = argparse.ArgumentParser(
        description="Evaluate regex and local NER PII detectors on dbt marts."
    )
    parser.add_argument(
        "--eval-run-id",
        required=True,
        help="Versioned run id, e.g. 20261126T061500Z_ab12cd3",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=None,
        help="Maximum sampled documents (default PII_EVAL_MAX_DOCS=4000)",
    )
    parser.add_argument(
        "--bootstrap",
        type=int,
        default=None,
        help="Bootstrap iterations for F1 CIs (default PII_EVAL_BOOTSTRAP=1000)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point used by ``python -m ml.evaluate_pii_detectors``."""
    parser = build_parser()
    args = parser.parse_args(argv)
    from common.config import get_settings

    settings = get_settings()
    if args.max_docs is None:
        args.max_docs = settings.eval_max_docs
    if args.bootstrap is None:
        args.bootstrap = settings.eval_bootstrap
    try:
        return _run_evaluation(args)
    except EvaluationError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 - actionable CLI failure
        print(
            f"ERROR: evaluation failed ({type(error).__name__}): {error}\n"
            "Check PII_DB_HOST/PORT/NAME/USER/PASSWORD and that "
            "`docker compose up -d postgres` plus dbt have run.",
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
