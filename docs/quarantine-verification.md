# Local quarantine verification

The sample and flood demonstrations were run against an isolated PostgreSQL
database in the Docker Compose warehouse. Each batch was loaded with
`python -m ingestion.inject_bad_records`, transformed with `dbt build`, and then
passed to the triage CLI using the recorded result of that dbt command.

| Demonstration | Raw rows inspected | Quarantined | Validated leaks | dbt result | Review tasks |
| --- | ---: | ---: | ---: | --- | ---: |
| Sample | 204 | 5 | 0 | 66 passed | 1 slice alert |
| Flood | 201 | 28 | 0 | Quarantine rate test failed | 0 |

The sample quarantined both copies of the duplicate key and one each of the
out-of-bounds span, null label, and invalid OCR confidence. The flood
quarantined 13.93% of inspected rows, exceeding the 5% threshold. Calling the
agent directly after recording the failed result produced one
`BLOCKED_DQ_FAIL` audit row. These isolated demonstrations did not modify the
Airflow scale-run warehouse.

The Streamlit reviewer approved the sample queue task against the same database.
The queue row changed from `OPEN` to `APPROVED` and received a review timestamp.
