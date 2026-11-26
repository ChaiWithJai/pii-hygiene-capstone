select
    s.span_key,
    s.document_key,
    s.start_offset,
    s.end_offset,
    s.span_value
from {{ ref('fct_pii_spans') }} s
inner join {{ ref('stg_pii_documents') }} d
    on d.document_key = s.document_key
   and d.batch_id = s.batch_id
where substring(d.raw_text from s.start_offset + 1 for s.end_offset - s.start_offset)
    <> s.span_value
