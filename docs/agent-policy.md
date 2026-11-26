# Triage agent policy

The triage agent is deterministic and quality-gated. It is not an autonomous data
cleaner and does not modify raw or mart records. Its state-changing scope is limited
to the governance review queue and, only when explicitly configured, a sandbox
GitHub issue.

## Preconditions

Before reading candidates, the agent checks the latest
`governance.dbt_run_metadata.test_status`. Only `PASS` permits candidate processing.
On any other state it writes one `BLOCKED_DQ_FAIL` audit event and exits without
creating review tasks. Airflow also skips the task when upstream dbt tests fail.

Candidates come only from `marts.vw_agent_triage_candidates`; raw and quarantine
tables are not candidate sources. Each decision writes evidence, decision, action,
outcome, OpenTelemetry trace ID, and execution timestamp to the immutable
`governance.agent_action_log`.

## Allowlisted decisions

| Candidate type | Decision | Action | State change |
|---|---|---|---|
| `OCR_VERIFICATION` | `OPEN_OCR_VERIFICATION_TASK` | `INSERT_REVIEW_QUEUE` | Create an `OCR_VERIFICATION` task. |
| `REDACTION_REVIEW` | `OPEN_REDACTION_REVIEW_TASK` | `INSERT_REVIEW_QUEUE` | Create a `REDACTION_REVIEW` task. |
| `SLICE_QUARANTINE_ALERT` | `QUARANTINE_SLICE_ALERT` | `INSERT_SLICE_ALERT` | Create a `SLICE_QUARANTINE_ALERT` task. |
| `VERIFIED_FALSE_POSITIVE` | `HOLD_AND_LOG` | `NO_STATE_CHANGE` | Audit only. |

No arbitrary SQL, shell, messaging, deletion, or mart mutation tool is allowed.
Payloads are validated before execution. `--max-actions` and
`PII_AGENT_MAX_ACTIONS` bound each run.

## Idempotency and human review

Task and action identity derive from SHA-256 of
`document_key|task_type|batch_id` (first 32 hexadecimal characters for `task_id`).
The action log enforces a unique idempotency key. A retry therefore records or
reports `SKIPPED_IDEMPOTENT` rather than opening a second task.

Review tasks start `OPEN` and are assigned to `PRIVACY_REVIEWER`. In Streamlit, a
reviewer may set the status to `APPROVED`, `DISMISSED`, or `ESCALATED`, with reviewer
identity, time, and note. These are human workflow decisions; approval does not
automatically redact data or retrain a model.

## Optional external action

Sandbox GitHub issue creation is disabled unless both `SANDBOX_GITHUB_REPO` and
`SANDBOX_GITHUB_TOKEN` are set. When enabled, the repository must be a nonproduction
sandbox and issues carry the `pii-sandbox` label. Evidence must not include raw PII
values or credentials. A failed external call must remain visible in the action
outcome and must not be silently retried outside the normal idempotent workflow.
