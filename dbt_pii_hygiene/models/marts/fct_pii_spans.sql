select
    {{ pii_hash("concat_ws('|', s.document_key, cast(s.span_index as text))") }} as span_key,
    s.document_key,
    d.batch_id,
    e.entity_type_key,
    s.raw_entity_label,
    s.start_offset,
    s.end_offset,
    s.span_value,
    (
        s.start_offset >= 0
        and s.end_offset > s.start_offset
        and s.end_offset <= d.text_char_length
        and substr(sd.raw_text, s.start_offset + 1, s.end_offset - s.start_offset) = s.span_value
    ) as offset_valid_flag,
    e.privacy_tier
from {{ ref('stg_pii_spans') }} as s
inner join {{ ref('fct_pii_documents') }} as d
    on s.document_key = d.document_key
    and s.batch_id = d.batch_id
inner join {{ ref('stg_pii_documents') }} as sd
    on s.document_key = sd.document_key
    and s.batch_id = sd.batch_id
left join {{ ref('dim_entity_type') }} as e
    on lower(s.raw_entity_label) = lower(e.raw_entity_label)
