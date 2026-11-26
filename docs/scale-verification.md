# Docker Compose scale verification

Airflow run `scale-verification` (`batch_id=scale_verification`) completed all
nine tasks successfully on November 26, 2026. The final dbt gate passed **43
tests**, with zero failures. The run was resumed from failed tasks after
integration fixes; previously completed extraction and OCR were reused.

| Warehouse result | Observed |
|---|---:|
| Loaded structured rows | 1,376,609 |
| Validated document rows / distinct document keys | 1,375,254 / 1,375,254 |
| Validated spans | 10,196,253 |
| Quarantined substring mismatches | 1,355 |
| Quarantined keys in validated documents or review tasks | 0 |
| OCR PDFs / PNGs | 317 / 183 |
| Validated OCR extractions | 5,980 |
| Invalid page bounds or confidence scores | 0 |
| Agent audit rows / review tasks created | 40 / 39 |

The validated document count includes 1,309,891 OpenPII documents, 48,645
Nemotron documents, 8,688 SPY documents, and 8,030 synthetic CSV stand-ins
for gated PII-SCOPE. This run staged Nemotron before locale-qualified keys
were added, so it retained only one row per upstream UID. The measured
warehouse count above reflects that staged batch.

The two detectors scored 591 held-out documents each. The regex baseline
produced 130 metric rows and the multilingual spaCy model (`xx_ent_wiki_sm`)
produced 145. Both produced language, modality, privacy-tier, and overall
slices with F1 confidence intervals. Median per-document inference latency
was 0.05 ms for regex and 1.29 ms for spaCy on the test host.

The agent logged 39 successful redaction review queue writes and one
`HOLD_AND_LOG`. Streamlit displayed the distinct validated document count
and rendered a source-page preview with its bounding box.

```sql
select count(*), count(distinct document_key)
from marts.fct_pii_documents
where batch_id = 'scale_verification';

select model_name, count(*) as metric_rows
from marts.fct_ml_eval_slices
group by model_name;

select a.decision, a.outcome, count(*) as audit_rows, count(q.task_id) as queue_rows
from governance.agent_action_log a
left join governance.agent_review_queue q on q.task_id = a.task_id
where a.batch_id = 'scale_verification'
group by a.decision, a.outcome;
```

See [quarantine verification](quarantine-verification.md) for the isolated
bad-record and circuit-breaker demonstrations.
