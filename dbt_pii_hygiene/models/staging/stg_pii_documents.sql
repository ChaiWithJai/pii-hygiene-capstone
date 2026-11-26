{{ config(materialized='view') }}

select
    batch_id,
    document_key,
    source_dataset,
    upper(source_format) as source_format,
    record_id,
    raw_text,
    spans_json,
    nullif(trim(language), '') as language,
    nullif(trim(domain), '') as domain,
    template_hash,
    ingested_at
from {{ source('raw', 'pii_documents_raw') }}
