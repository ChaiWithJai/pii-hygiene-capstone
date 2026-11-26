{{ config(materialized='view') }}

select
    eval_run_id,
    model_name,
    split,
    slice_type,
    slice_value,
    entity_type,
    precision,
    recall,
    f1,
    pr_auc,
    support,
    tp,
    fp,
    fn,
    ci_low_f1,
    ci_high_f1,
    evaluated_at
from {{ source('raw', 'ml_eval_slices_raw') }}
