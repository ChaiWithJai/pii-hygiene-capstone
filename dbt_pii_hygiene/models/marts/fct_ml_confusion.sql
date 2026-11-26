{{ config(materialized='view') }}

select
    eval_run_id,
    model_name,
    split,
    slice_type,
    slice_value,
    entity_type,
    tp,
    fp,
    fn,
    tn,
    evaluated_at
from {{ source('raw', 'ml_confusion_raw') }}
