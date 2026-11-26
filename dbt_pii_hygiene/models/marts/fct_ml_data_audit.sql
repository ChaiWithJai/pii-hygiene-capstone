{{ config(materialized='view') }}

select
    eval_run_id,
    table_name,
    column_name,
    null_count,
    total_count,
    null_fraction,
    handling,
    noted_at
from {{ source('raw', 'ml_data_audit_raw') }}
