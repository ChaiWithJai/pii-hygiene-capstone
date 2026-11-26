"""Deterministic synthetic fixture and template pools.

The real ``Protegrity/pii-scope`` dataset is gated on Hugging Face and cannot be
redistributed by this repository.  ``make_pii_scope_fixture`` therefore produces
a clearly labelled **synthetic stand-in** with the same public CSV columns
(``id,text,privacy_mask,language,domain``) and realistic dense PII spans.  The
fixture is never written under the real dataset name: ``download_sources`` only
ever writes ``pii_scope/pii_scope.csv`` for a real, authorised download.

Template pools in this module are also used by
``ingestion.generate_unstructured_docs`` to render clinical notes, invoices and
legal notices.  Every generated value is fake; names, identifiers and accounts
do not correspond to real people.
"""

from __future__ import annotations

import csv
import json
import os
import random
import re
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from ingestion import atomic_write_text, compute_template_hash, make_span

SYNTHETIC_DATASETS: dict[str, str] = {
    "openpii": "synthetic-fixture/openpii-jsonl",
    "nemotron": "synthetic-fixture/nemotron-parquet",
    "pii_scope": "synthetic-fixture/pii-scope-csv",
    "spy": "synthetic-fixture/spy-jsonl",
}

# ---------------------------------------------------------------------------
# PII value generators (all values are synthetic).
# ---------------------------------------------------------------------------

_FIRST_NAMES = (
    "Maria",
    "James",
    "Wei",
    "Aisha",
    "Diego",
    "Priya",
    "Noah",
    "Fatima",
    "Lucas",
    "Sofia",
    "Ethan",
    "Yuki",
    "Amara",
    "Omar",
    "Elena",
    "Kwame",
)
_LAST_NAMES = (
    "Gonzalez",
    "O'Connor",
    "Chen",
    "Patel",
    "Hernandez",
    "Nakamura",
    "Mbeki",
    "Kowalski",
    "Nguyen",
    "Silva",
    "Andersson",
    "Rahman",
)
_ORGANIZATIONS = (
    "Northside Medical Center",
    "Lakeshore Credit Union",
    "Apex Logistics LLC",
    "Cedar Valley Hospital",
    "Brightline Insurance Group",
    "Harbor Legal Partners",
    "Summit Family Clinic",
    "Metro Utilities Authority",
)
_CITIES = (
    ("Springfield", "IL", "62704"),
    ("Austin", "TX", "78701"),
    ("Portland", "OR", "97205"),
    ("Raleigh", "NC", "27601"),
    ("Denver", "CO", "80202"),
    ("Tampa", "FL", "33602"),
)
_STREETS = (
    "87 Avenida De La Estrella",
    "1420 Maple Grove Road",
    "55 Harbor View Terrace",
    "903 Cedar Lane",
    "2718 Sunset Boulevard",
    "64 Willow Creek Drive",
)
_SERVICES = (
    "annual physical examination",
    "cardiology consultation",
    "laboratory panel",
    "outpatient procedure",
    "follow-up assessment",
    "imaging review",
)
_MONTHS = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def _person(rng: random.Random) -> str:
    return f"{rng.choice(_FIRST_NAMES)} {rng.choice(_LAST_NAMES)}"


def _email(rng: random.Random) -> str:
    first = rng.choice(_FIRST_NAMES).lower()
    last = re.sub(r"[^a-z]", "", rng.choice(_LAST_NAMES).lower())
    return f"{first}.{last}{rng.randint(1, 99)}@example.com"


def _phone(rng: random.Random) -> str:
    return f"({rng.randint(200, 989)}) {rng.randint(200, 989)}-{rng.randint(1000, 9999)}"


def _ssn(rng: random.Random) -> str:
    return f"{rng.randint(100, 899)}-{rng.randint(10, 99)}-{rng.randint(1000, 9999)}"


def _card(rng: random.Random) -> str:
    prefix = rng.choice(("4111", "5500", "3400", "6011"))
    groups = [prefix] + [f"{rng.randint(0, 9999):04d}" for _ in range(3)]
    return " ".join(groups)


def _mrn(rng: random.Random) -> str:
    return f"MRN-{rng.randint(10000, 99999)}"


def _account(rng: random.Random) -> str:
    return f"ACC-{rng.randint(100000, 999999)}"


def _date(rng: random.Random) -> str:
    return f"{rng.choice(_MONTHS)} {rng.randint(1, 28)}, {rng.randint(1950, 2005)}"


def _iso_date(rng: random.Random) -> str:
    return f"{rng.randint(1950, 2005)}-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}"


def _address(rng: random.Random) -> str:
    city, state, zip_code = rng.choice(_CITIES)
    return f"{rng.choice(_STREETS)}, {city}, {state} {zip_code}"


def _city_state(rng: random.Random) -> str:
    city, state, _ = rng.choice(_CITIES)
    return f"{city}, {state}"


def _zip(rng: random.Random) -> str:
    return rng.choice(_CITIES)[2]


def _url(rng: random.Random) -> str:
    return f"https://portal.example.com/case/{rng.randint(10000, 99999)}"


def _ip(rng: random.Random) -> str:
    return f"10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"


def _license(rng: random.Random) -> str:
    return f"{rng.choice('ABCDEFGHJKLMNPRSTUVWXYZ')}{rng.randint(100, 999)}-{rng.randint(1000, 9999)}"


def _passport(rng: random.Random) -> str:
    return f"P{rng.randint(10000000, 99999999)}"


def _iban(rng: random.Random) -> str:
    return f"GB{rng.randint(10, 99)}BARC{rng.randint(10000000, 99999999)}"


def _username(rng: random.Random) -> str:
    return f"{rng.choice(_FIRST_NAMES).lower()}_{rng.randint(10, 999)}"


def _age(rng: random.Random) -> str:
    return f"{rng.randint(18, 89)} years old"


def _amount(rng: random.Random) -> str:
    return f"${rng.randint(25, 9500)}.{rng.randint(10, 99):02d}"


_GENERATORS = {
    "PERSON": _person,
    "EMAIL": _email,
    "PHONE": _phone,
    "SSN": _ssn,
    "CREDIT_CARD": _card,
    "MRN": _mrn,
    "ACCOUNT_NUMBER": _account,
    "DATE": _date,
    "ISO_DATE": _iso_date,
    "ADDRESS": _address,
    "CITY_STATE": _city_state,
    "ZIP": _zip,
    "ORGANIZATION": lambda rng: rng.choice(_ORGANIZATIONS),
    "SERVICE": lambda rng: rng.choice(_SERVICES),
    "URL": _url,
    "IP_ADDRESS": _ip,
    "DRIVER_LICENSE": _license,
    "PASSPORT": _passport,
    "IBAN": _iban,
    "USERNAME": _username,
    "AGE": _age,
    "AMOUNT": _amount,
}

_PLACEHOLDER = re.compile(r"\{([A-Z_]+)\}")


def render_template(template: str, rng: random.Random) -> tuple[str, list[dict[str, Any]]]:
    """Fill ``{TOKEN}`` placeholders and return text plus aligned spans.

    The returned span values are exact slices of ``text`` so downstream dbt
    substring-alignment tests pass on the synthetic fixture.
    """
    parts: list[str] = []
    spans: list[dict[str, Any]] = []
    cursor = 0
    length = 0
    for match in _PLACEHOLDER.finditer(template):
        literal = template[cursor : match.start()]
        parts.append(literal)
        length += len(literal)
        token = match.group(1)
        generator = _GENERATORS.get(token)
        value = generator(rng) if generator else f"[{token}]"
        start = length
        parts.append(value)
        length += len(value)
        if generator is not None:
            spans.append(make_span(start, length, value, token))
        cursor = match.end()
    tail = template[cursor:]
    parts.append(tail)
    return "".join(parts), spans


def render_template_sequence(
    templates: Sequence[str], rng: random.Random
) -> tuple[str, list[dict[str, Any]]]:
    """Render several templates and join them with blank lines."""
    chunks: list[str] = []
    spans: list[dict[str, Any]] = []
    offset = 0
    for index, template in enumerate(templates):
        if index:
            chunks.append("\n\n")
            offset += 2
        text, local_spans = render_template(template, rng)
        chunks.append(text)
        spans.extend(
            make_span(span["start"] + offset, span["end"] + offset, span["value"], span["label"])
            for span in local_spans
        )
        offset += len(text)
    return "".join(chunks), spans


# ---------------------------------------------------------------------------
# PII-SCOPE synthetic fixture.
# ---------------------------------------------------------------------------

PII_SCOPE_LANGUAGES = ("en",)
PII_SCOPE_DOMAINS = ("healthcare", "finance", "legal", "government", "education", "insurance")

_PII_SCOPE_PARAGRAPHS: dict[str, tuple[str, ...]] = {
    "healthcare": (
        "Patient {PERSON} (DOB {DATE}; MRN {MRN}) was seen at {ORGANIZATION} for a "
        "{SERVICE}. Direct contact is {PHONE}; the patient portal email is {EMAIL}.",
        "Insurance member {ACCOUNT_NUMBER} was verified for {PERSON}. The guarantor "
        "resides at {ADDRESS} and can be reached at {PHONE} on weekdays.",
        "Referring physician {PERSON} requested {SERVICE}. Prior records were sent to "
        "{EMAIL} and the billing office at {ORGANIZATION} was copied on {DATE}.",
        "Laboratory results for {PERSON} were released through {URL}. The specimen "
        "accession number is {MRN}; date of collection was {ISO_DATE}.",
        "Emergency contact for {PERSON} is {PERSON}, relationship recorded as spouse, "
        "telephone {PHONE}, alternate email {EMAIL}, address {ADDRESS}.",
        "This addendum corrects the account holder for {ACCOUNT_NUMBER} to {PERSON}. "
        "The original intake form listed {ADDRESS} and date of birth {DATE}.",
        "A prescription was authorized by {PERSON} on {DATE} and transmitted to the "
        "pharmacy network at {ORGANIZATION}. Member identifier {MRN}.",
    ),
    "finance": (
        "Account holder {PERSON} (customer id {ACCOUNT_NUMBER}) updated the mailing "
        "address to {ADDRESS}. Verification call was placed to {PHONE}.",
        "Cardholder {PERSON} reported a transaction on card {CREDIT_CARD} at "
        "{ORGANIZATION}. The registered email is {EMAIL} and SSN ending was {SSN}.",
        "Wire instructions for {PERSON} reference IBAN {IBAN}. Confirmation was sent "
        "to {EMAIL} on {DATE}; callback number {PHONE}.",
        "Loan application {ACCOUNT_NUMBER} for {PERSON}, born {ISO_DATE}, lists income "
        "verification through {ORGANIZATION}. Applicant address is {ADDRESS}.",
        "Beneficiary {PERSON} submitted identity document {PASSPORT} and driver licence "
        "{DRIVER_LICENSE}. The bank may contact the customer at {PHONE}.",
        "Fraud review case {ACCOUNT_NUMBER} involves {PERSON} and the device IP "
        "{IP_ADDRESS}. Analyst notes were attached from {EMAIL} on {DATE}.",
        "Statement delivery for {PERSON} is configured for {EMAIL}. The billing address "
        "on file is {ADDRESS} and the secondary phone is {PHONE}.",
    ),
    "legal": (
        "NOTICE TO {PERSON}, residing at {ADDRESS}: a filing referencing account "
        "{ACCOUNT_NUMBER} was recorded on {DATE} in the matter of {ORGANIZATION}.",
        "Client {PERSON} (DOB {ISO_DATE}) authorized {ORGANIZATION} to release records "
        "to {EMAIL}. Questions may be directed to counsel at {PHONE}.",
        "Witness {PERSON}, contact {PHONE}, provided a statement concerning document "
        "{PASSPORT} and the events of {DATE}. A copy was mailed to {ADDRESS}.",
        "The court file lists {PERSON} under case {ACCOUNT_NUMBER}. Supporting evidence "
        "was uploaded to {URL}; SSN {SSN} appears in the sealed attachment.",
        "Settlement correspondence for {PERSON} was sent to {ADDRESS} and copied to "
        "{EMAIL}. The escrow reference is {IBAN} and the review date is {DATE}.",
        "Affidavit of {PERSON}, sworn on {DATE}, identifies employer {ORGANIZATION} "
        "and a contact telephone number of {PHONE}. Identification {DRIVER_LICENSE}.",
        "Counsel for {PERSON} requested the complete file from {ORGANIZATION}. Service "
        "was accepted by {PERSON} at {EMAIL} on {ISO_DATE}.",
    ),
    "government": (
        "Benefit application for {PERSON}, born {ISO_DATE}, lists residence at "
        "{ADDRESS}. The case worker may be reached at {PHONE} or {EMAIL}.",
        "Permit {ACCOUNT_NUMBER} was issued to {PERSON} on {DATE}. The applicant "
        "provided identity document {DRIVER_LICENSE} and mailing address {ADDRESS}.",
        "Tax correspondence for {PERSON} references identifier {ACCOUNT_NUMBER}. The "
        "notice was sent to {ADDRESS} and a duplicate emailed to {EMAIL}.",
        "Public record request from {PERSON} concerns parcel records at {ADDRESS}. "
        "Contact telephone {PHONE}; identity verified with passport {PASSPORT}.",
        "Registration {ACCOUNT_NUMBER} for {PERSON} expires on {DATE}. The responsible "
        "agency is {ORGANIZATION} and the service portal is {URL}.",
        "Complaint filed by {PERSON} on {DATE} names witness {PERSON} of {ADDRESS}. "
        "Reach the complainant at {PHONE} or {EMAIL}.",
    ),
    "education": (
        "Student {PERSON} (DOB {ISO_DATE}) is enrolled at {ORGANIZATION}. Guardian "
        "contact is {PHONE} and the family email is {EMAIL}.",
        "Financial aid file {ACCOUNT_NUMBER} for {PERSON} lists household address "
        "{ADDRESS}. Verification documents were sent on {DATE}.",
        "Transcript request for {PERSON} was received from {EMAIL}. The student record "
        "identifier is {MRN} and the mailing address is {ADDRESS}.",
        "Alumni update from {PERSON}: employer {ORGANIZATION}, phone {PHONE}, and "
        "preferred contact address {ADDRESS}. Preferred name on file {USERNAME}.",
        "Disciplinary notice {ACCOUNT_NUMBER} concerning {PERSON} was issued on {DATE}. "
        "Parent or guardian {PERSON} can be reached at {PHONE}.",
    ),
    "insurance": (
        "Claim {ACCOUNT_NUMBER} for {PERSON} (DOB {DATE}) lists treating provider "
        "{ORGANIZATION}. Contact the policyholder at {PHONE} or {EMAIL}.",
        "Policyholder {PERSON} updated beneficiary details for account {ACCOUNT_NUMBER}. "
        "The mailing address is {ADDRESS} and identity document {PASSPORT} was verified.",
        "Adjuster {PERSON} scheduled an inspection at {ADDRESS} on {DATE}. The claimant "
        "telephone number is {PHONE} and email is {EMAIL}.",
        "Reimbursement for {PERSON} references card {CREDIT_CARD} and bank account "
        "{IBAN}. Supporting invoices were sent to {EMAIL}.",
        "Underwriting file for {PERSON}, resident at {ADDRESS}, includes SSN {SSN} and "
        "driver licence {DRIVER_LICENSE}. Review completed on {DATE}.",
    ),
}

_FILLER = (
    "This synthetic record is provided solely to exercise schema validation, "
    "offset alignment and data quality routing in the local capstone pipeline. "
    "It does not describe a real person, organisation or transaction."
)


def make_pii_scope_fixture(n: int = 8030) -> Iterator[dict[str, Any]]:
    """Yield ``n`` deterministic synthetic PII-SCOPE-shaped rows.

    Columns match the plausible gated CSV schema: ``id,text,privacy_mask,
    language,domain``.  ``privacy_mask`` is a list of JSON-serialisable span
    dicts.  The generator uses a local ``random.Random(42)`` instance so the
    first ``n`` rows are stable across processes.
    """
    rng = random.Random(42)
    domains = list(_PII_SCOPE_PARAGRAPHS)
    for index in range(n):
        domain = domains[index % len(domains)]
        pool = _PII_SCOPE_PARAGRAPHS[domain]
        paragraph_count = rng.randint(5, 7)
        paragraphs = [rng.choice(pool) for _ in range(paragraph_count)]
        paragraphs.append(_FILLER)
        text, spans = render_template_sequence(paragraphs, rng)
        yield {
            "id": f"synth-pii-scope-{index + 1:05d}",
            "text": text,
            "privacy_mask": spans,
            "language": PII_SCOPE_LANGUAGES[0],
            "domain": domain,
        }


def write_pii_scope_fixture_csv(path: Path, n: int = 8030) -> int:
    """Write the synthetic PII-SCOPE fixture as an atomic CSV file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    count = 0
    try:
        with tmp.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=["id", "text", "privacy_mask", "language", "domain"]
            )
            writer.writeheader()
            for row in make_pii_scope_fixture(n):
                writer.writerow(
                    {
                        "id": row["id"],
                        "text": row["text"],
                        "privacy_mask": json.dumps(row["privacy_mask"], ensure_ascii=False),
                        "language": row["language"],
                        "domain": row["domain"],
                    }
                )
                count += 1
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
    return count


def make_offline_source_rows(rows_per_source: int = 3) -> dict[str, list[dict[str, Any]]]:
    """Return small deterministic stand-ins for all structured source shapes.

    Dataset identities are deliberately different from the real Hugging Face
    repositories.  This prevents an offline fixture from being mistaken for an
    authorised copy of a gated or downloaded corpus.
    """
    rows: dict[str, list[dict[str, Any]]] = {name: [] for name in SYNTHETIC_DATASETS}
    for source_index, source in enumerate(rows):
        rng = random.Random(42 + source_index)
        for index in range(rows_per_source):
            template = (
                "Synthetic contact {PERSON} can be reached at {EMAIL} or {PHONE}. "
                "Reference {ACCOUNT_NUMBER}."
            )
            text, spans = render_template(template, rng)
            prefix = "synth" if source == "pii_scope" else "fixture"
            record_id = f"{prefix}-{source}-{index + 1:03d}"
            rows[source].append(
                {
                    "id": record_id,
                    "uid": record_id,
                    "text": text,
                    "raw_text": text,
                    "masked_text": template,
                    "text_tagged": template,
                    "privacy_mask": spans,
                    "spans": spans,
                    "language": "en",
                    "domain": "fixture",
                    "source_dataset": SYNTHETIC_DATASETS[source],
                }
            )
    return rows


def write_offline_source_fixtures(raw_dir: Path, rows_per_source: int = 3) -> dict[str, Path]:
    """Write deterministic JSONL, Parquet, and CSV source fixtures.

    The filenames are intentionally prefixed with ``fixture_`` so production
    source discovery cannot silently confuse them with real downloads.
    """
    import pandas as pd

    raw_dir.mkdir(parents=True, exist_ok=True)
    rows = make_offline_source_rows(rows_per_source)
    paths = {
        "openpii": raw_dir / "fixture_openpii.jsonl",
        "nemotron": raw_dir / "fixture_nemotron.parquet",
        "pii_scope": raw_dir / "fixture_pii_scope.csv",
        "spy": raw_dir / "fixture_spy.jsonl",
    }
    for source in ("openpii", "spy"):
        payload = "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows[source])
        atomic_write_text(paths[source], payload)
    parquet_rows = [
        {**row, "privacy_mask": json.dumps(row["privacy_mask"]), "spans": json.dumps(row["spans"])}
        for row in rows["nemotron"]
    ]
    paths["nemotron"].parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(parquet_rows).to_parquet(paths["nemotron"], index=False)
    with paths["pii_scope"].open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "id",
                "text",
                "privacy_mask",
                "language",
                "domain",
                "source_dataset",
                "masked_text",
            ],
        )
        writer.writeheader()
        for row in rows["pii_scope"]:
            writer.writerow(
                {
                    "id": row["id"],
                    "text": row["text"],
                    "privacy_mask": json.dumps(row["privacy_mask"]),
                    "language": row["language"],
                    "domain": row["domain"],
                    "source_dataset": row["source_dataset"],
                    "masked_text": row["masked_text"],
                }
            )
    return paths


# ---------------------------------------------------------------------------
# Unstructured document template pools (used by generate_unstructured_docs).
# ---------------------------------------------------------------------------

DOC_TYPE_META: dict[str, dict[str, str]] = {
    "clinical_note": {"domain": "healthcare", "language": "en"},
    "invoice": {"domain": "finance", "language": "en"},
    "legal_notice": {"domain": "legal", "language": "en"},
}

DOCUMENT_TEMPLATE_POOLS: dict[str, tuple[str, ...]] = {
    "clinical_note": (
        "NORTHSIDE MEDICAL CENTER\nOUTPATIENT CLINICAL NOTE\n\n"
        "Patient: {PERSON}\nDate of birth: {DATE}\nMedical record number: {MRN}\n"
        "Address: {ADDRESS}\nPhone: {PHONE}\nEmail: {EMAIL}\n\n"
        "Visit date: {DATE}\nAttending provider: {PERSON}\nService: {SERVICE}\n\n"
        "History: The patient was seen at {ORGANIZATION} for a {SERVICE}. The patient "
        "reports intermittent symptoms over the past several weeks. Current insurance "
        "member identifier is {ACCOUNT_NUMBER}. Emergency contact is {PERSON} at "
        "{PHONE}.\n\nAssessment: Stable. Laboratory studies were ordered and the "
        "results portal is {URL}.\n\nPlan: Follow up in four weeks. Questions may be "
        "directed to the clinic at {PHONE} or {EMAIL}. This note is synthetic and does "
        "not describe a real patient.",
        "CEDAR VALLEY HOSPITAL\nDISCHARGE SUMMARY\n\n"
        "Patient name: {PERSON}\nDOB: {ISO_DATE}\nMRN: {MRN}\n"
        "Mailing address: {ADDRESS}\nTelephone: {PHONE}\n\n"
        "Admission date: {DATE}\nDischarge date: {DATE}\nPrimary physician: {PERSON}\n"
        "Facility: {ORGANIZATION}\n\n"
        "The patient was admitted for a {SERVICE}. Records were released to {EMAIL} "
        "with authorisation from {PERSON}. Billing account {ACCOUNT_NUMBER} remains "
        "active. The patient can be reached at {PHONE} after discharge.\n\n"
        "Discharge instructions: Complete the prescribed course of treatment and "
        "schedule a follow-up appointment. Portal: {URL}.\n\n"
        "Synthetic document generated for OCR quality testing only.",
        "SUMMIT FAMILY CLINIC\nREFERRAL LETTER\n\n"
        "To: {ORGANIZATION}\nFrom: {PERSON}, MD\nPatient: {PERSON}\n"
        "Date of birth: {DATE}\nMedical record number: {MRN}\n"
        "Phone: {PHONE}\nEmail: {EMAIL}\nAddress: {ADDRESS}\n\n"
        "Dear colleague, I am referring the above patient for a {SERVICE}. The patient "
        "has been evaluated at our clinic and reports ongoing symptoms. Insurance "
        "member id {ACCOUNT_NUMBER} is active. Please contact the patient directly at "
        "{PHONE} or {EMAIL} to arrange scheduling.\n\n"
        "Supporting records have been uploaded to {URL}. If additional information is "
        "needed, contact our office at {PHONE}.\n\n"
        "Synthetic referral letter for testing purposes.",
    ),
    "invoice": (
        "APEX LOGISTICS LLC\nINVOICE\n\n"
        "Invoice number: {ACCOUNT_NUMBER}\nInvoice date: {DATE}\nDue date: {DATE}\n\n"
        "Bill to:\n{PERSON}\n{ADDRESS}\nEmail: {EMAIL}\nPhone: {PHONE}\n\n"
        "Customer account: {ACCOUNT_NUMBER}\nTax identifier: {SSN}\n\n"
        "Description: {SERVICE}\nAmount: {AMOUNT}\nShipping reference: {URL}\n\n"
        "Remit payment to {ORGANIZATION}. For billing questions contact {PERSON} at "
        "{PHONE} or {EMAIL}. Card on file: {CREDIT_CARD}.\n\n"
        "Synthetic invoice generated for OCR quality testing. No payment is due.",
        "LAKESHORE CREDIT UNION\nSTATEMENT OF ACCOUNT\n\n"
        "Statement period: {DATE} to {DATE}\nMember: {PERSON}\n"
        "Member number: {ACCOUNT_NUMBER}\nAddress: {ADDRESS}\n"
        "Email: {EMAIL}\nTelephone: {PHONE}\n\n"
        "Beginning balance: {AMOUNT}\nEnding balance: {AMOUNT}\n\n"
        "Recent activity includes a card transaction at {ORGANIZATION} using card "
        "{CREDIT_CARD}. A wire transfer reference {IBAN} was posted. Identity "
        "verification on file includes passport {PASSPORT} and driver licence "
        "{DRIVER_LICENSE}.\n\n"
        "To dispute a transaction call {PHONE} or write to {EMAIL}. Member service "
        "portal: {URL}.\n\n"
        "Synthetic statement generated for testing; not a real financial record.",
        "BRIGHTLINE INSURANCE GROUP\nPREMIUM NOTICE\n\n"
        "Policyholder: {PERSON}\nPolicy number: {ACCOUNT_NUMBER}\n"
        "Date of birth: {DATE}\nAddress: {ADDRESS}\n"
        "Phone: {PHONE}\nEmail: {EMAIL}\n\n"
        "Amount due: {AMOUNT}\nDue date: {DATE}\n\n"
        "Your policy with {ORGANIZATION} is scheduled for renewal. The beneficiary on "
        "file is {PERSON}. Payment may be made by card {CREDIT_CARD} through the "
        "customer portal at {URL}. A cancellation request must be received before "
        "{DATE}.\n\n"
        "For assistance contact {PERSON} at {PHONE} or {EMAIL}.\n\n"
        "Synthetic premium notice for OCR testing only.",
    ),
    "legal_notice": (
        "HARBOR LEGAL PARTNERS\nTheodora House, 14 Harbor View Terrace\n\n"
        "NOTICE OF REPRESENTATION\n\n"
        "To: {PERSON}\nAddress: {ADDRESS}\nEmail: {EMAIL}\nPhone: {PHONE}\n\n"
        "Dear {PERSON},\n\n"
        "This notice confirms that our firm represents {PERSON} in connection with "
        "matter {ACCOUNT_NUMBER}. Correspondence should be directed to {PERSON} at "
        "{EMAIL}. The client's date of birth is {DATE} and the identification on file "
        "is driver licence {DRIVER_LICENSE}.\n\n"
        "A scheduling conference has been set for {DATE}. Please preserve all records "
        "relating to {ORGANIZATION}. The secure document portal is {URL}.\n\n"
        "If you are represented by counsel, please forward this notice immediately. "
        "This is a synthetic legal notice, not legal advice.",
        "SUPERIOR COURT OF THE STATE\nCIVIL DIVISION\n\n"
        "Case number: {ACCOUNT_NUMBER}\nPlaintiff: {PERSON}\nDefendant: {ORGANIZATION}\n"
        "Notice date: {DATE}\n\n"
        "TO: {PERSON}, residing at {ADDRESS}.\n\n"
        "You are notified that a civil action has been filed concerning an account "
        "held by {ORGANIZATION}. The plaintiff's contact details are {PHONE} and "
        "{EMAIL}. Documents may be reviewed at {URL}. A sealed exhibit contains SSN "
        "{SSN} and passport {PASSPORT}.\n\n"
        "You must respond on or before {DATE}. Failure to respond may result in a "
        "default judgment. Contact the clerk at {PHONE} for accommodations.\n\n"
        "Synthetic court notice generated for OCR pipeline testing.",
        "METRO UTILITIES AUTHORITY\n\n"
        "NOTICE OF LIEN AND RIGHT TO CURE\n\n"
        "Account holder: {PERSON}\nAccount number: {ACCOUNT_NUMBER}\n"
        "Service address: {ADDRESS}\nMailing address: {ADDRESS}\n"
        "Telephone: {PHONE}\nEmail: {EMAIL}\n\n"
        "Amount in arrears: {AMOUNT}\nNotice date: {DATE}\n\n"
        "Our records indicate an unpaid balance for service at the property identified "
        "above. Interested parties must contact {ORGANIZATION} at {PHONE} by {DATE} to "
        "avoid further action. Identity verification may require photo identification "
        "{DRIVER_LICENSE} and the last four digits of SSN {SSN}.\n\n"
        "Correspondence should be sent to {EMAIL}. Portal: {URL}.\n\n"
        "Synthetic lien notice generated for testing only.",
    ),
}


def choose_document_templates(
    doc_type: str, rng: random.Random, pages: int = 1
) -> list[str]:
    """Select one template per page for a document type."""
    pool = DOCUMENT_TEMPLATE_POOLS[doc_type]
    return [rng.choice(pool) for _ in range(max(1, pages))]


def document_template_hash(templates: Sequence[str]) -> str:
    """Hash the placeholder template(s), not the rendered fake values."""
    return compute_template_hash("\n\n".join(templates))


def apply_char_confusion(text: str, rng: random.Random, rate: float = 0.10) -> str:
    """Introduce deterministic OCR-style character confusion.

    Swaps a fraction of ``0/O``, ``1/l`` and ``5/S`` characters.  The function is
    deterministic for a given RNG state and leaves most text readable.
    """
    pairs = (("0", "O"), ("O", "0"), ("1", "l"), ("l", "1"), ("5", "S"), ("S", "5"))
    mapping = dict(pairs)
    out: list[str] = []
    for char in text:
        if char in mapping and rng.random() < rate:
            out.append(mapping[char])
        else:
            out.append(char)
    return "".join(out)


def write_fixture_header_only(path: Path) -> None:
    """Write a header-only PII-SCOPE CSV (useful for schema tests)."""
    atomic_write_text(path, "id,text,privacy_mask,language,domain\n")
