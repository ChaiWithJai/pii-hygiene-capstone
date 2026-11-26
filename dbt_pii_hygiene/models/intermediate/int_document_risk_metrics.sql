{{ config(materialized='table') }}

select
    batch_id,
    document_key,
    source_dataset,
    language,
    domain,
    template_hash,
    text_char_length,
    span_count,
    entity_density,
    has_overlapping_spans,
    has_high_risk_pii,
    dq_passed_flag,
    failure_reason,
    ingested_at
from {{ ref('int_pii_document_quality') }}
