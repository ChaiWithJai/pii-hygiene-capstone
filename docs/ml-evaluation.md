# ML evaluation

The evaluation compares a deterministic regex baseline with multilingual spaCy NER.
If `xx_ent_wiki_sm` is unavailable, the second model is a deterministic detector
reported as `contextual_lexicon_ner`; the fallback is never mislabeled as spaCy.

## Reproducibility and leakage control

Run:

```bash
python -m ml.evaluate_pii_detectors \
  --eval-run-id 20261126T061500Z_ab12cd3 \
  --max-docs 4000 \
  --bootstrap 1000
```

An evaluation ID is `<UTC timestamp compact>_<git sha 7>`. The evaluator reads only
validated marts and exits nonzero when they are empty. Before fitting or scoring,
it performs a 70/15/15 grouped split using `template_hash`, falling back to
`document_key`. It asserts zero template overlap between splits. Seeded sampling
and bootstrap operations make reruns reproducible for the same data and settings.

Each input column receives a missingness audit with a documented handling rule.
This audit is persisted even when missing values are retained rather than imputed.

## Metrics and slices

Entity predictions use exact character-span matching. Every model is evaluated by
precision, recall, F1, PR-AUC, support, TP, FP, and FN. F1 uncertainty is a 95%
document-level bootstrap confidence interval; the default is 1,000 iterations.
Operational results include median and p95 latency per document, documents/second,
peak RAM, artifact size, and evaluated document count.

Results are emitted for:

- overall performance;
- each privacy tier;
- `clean_structured_text` versus `ocr_perturbed_pdf` modality;
- `en` versus `non_en_multilingual` language;
- each entity type within those slices.

Raw results land in `raw.ml_data_audit_raw`, `raw.ml_eval_slices_raw`,
`raw.ml_confusion_raw`, and `raw.ml_operational_raw`; dbt promotes them to matching
`marts.fct_ml_*` relations. The Streamlit app displays accuracy intervals, confusion,
latency, and throughput without recomputing metrics.

## Interpretation

F1 alone is insufficient. Compare confidence intervals and support before claiming
a meaningful difference, inspect high-risk privacy tiers separately, and treat small
slices as directional. PR-AUC reflects ranking behavior and should not be compared
without the slice prevalence. Latency measurements are local-environment evidence,
not universal production capacity.

The benchmark contains synthetic and public benchmark data. It does not establish
fitness for handling real regulated records, clinical use, or automated enforcement.
