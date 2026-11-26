{{ config(materialized='view') }}

select
    eval_run_id,
    model_name,
    median_latency_ms,
    p95_latency_ms,
    docs_per_sec,
    peak_ram_mb,
    artifact_size_mb,
    docs_evaluated,
    evaluated_at
from {{ source('raw', 'ml_operational_raw') }}
