{{ config(materialized='view') }}

select
    d.batch_id,
    d.document_key,
    flattened.span_index,
    flattened.start_offset,
    flattened.end_offset,
    flattened.span_value,
    flattened.raw_entity_label
from {{ ref('stg_pii_documents') }} as d
{{ flatten_spans('d.spans_json') }}
