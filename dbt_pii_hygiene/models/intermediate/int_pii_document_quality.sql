{{ config(materialized='table') }}

with documents as (
    select
        d.*,
        count(*) over (
            partition by d.batch_id, d.document_key
        ) as batch_document_key_count
    from {{ ref('stg_pii_documents') }} as d
),

document_context as (
    select
        batch_id,
        document_key,
        max(raw_text) as raw_text
    from documents
    group by batch_id, document_key
),

mapped_spans as (
    select
        s.*,
        m.canonical_entity_label,
        m.privacy_tier,
        m.is_high_risk
    from {{ ref('stg_pii_spans') }} as s
    left join {{ ref('entity_taxonomy_map') }} as m
        on lower(trim(s.raw_entity_label)) = m.match_key
),

span_checks as (
    select
        d.batch_id,
        d.document_key,
        count(distinct s.span_index) as span_count,
        coalesce(
            min(case
                when s.span_index is null then 1
                when s.start_offset is not null
                    and s.end_offset is not null
                    and s.start_offset >= 0
                    and s.start_offset < s.end_offset
                    and s.end_offset <= char_length(d.raw_text)
                    then 1
                else 0
            end),
            1
        ) = 1 as are_offsets_valid,
        coalesce(
            min(case
                when s.span_index is null then 1
                when s.start_offset is not null
                    and s.end_offset is not null
                    and s.start_offset >= 0
                    and s.start_offset < s.end_offset
                    and s.end_offset <= char_length(d.raw_text)
                    and substr(
                        d.raw_text,
                        cast(s.start_offset + 1 as integer),
                        cast(s.end_offset - s.start_offset as integer)
                    ) = s.span_value
                    then 1
                else 0
            end),
            1
        ) = 1 as do_substrings_match,
        coalesce(
            min(case
                when s.span_index is null then 1
                when s.canonical_entity_label is not null then 1
                else 0
            end),
            1
        ) = 1 as are_labels_mapped,
        coalesce(max(case when s.is_high_risk = 1 then 1 else 0 end), 0) = 1
            as has_high_risk_pii
    from document_context as d
    left join mapped_spans as s
        on d.batch_id = s.batch_id
        and d.document_key = s.document_key
    group by d.batch_id, d.document_key, d.raw_text
),

overlap_checks as (
    select
        a.batch_id,
        a.document_key,
        1 as has_overlap
    from mapped_spans as a
    inner join mapped_spans as b
        on a.batch_id = b.batch_id
        and a.document_key = b.document_key
        and a.span_index < b.span_index
        and a.start_offset < b.end_offset
        and b.start_offset < a.end_offset
    group by a.batch_id, a.document_key
),

quality_flags as (
    select
        d.*,
        char_length(d.raw_text) as text_char_length,
        coalesce(s.span_count, 0) as span_count,
        case
            when char_length(d.raw_text) > 0
                then coalesce(s.span_count, 0) * 100.0 / char_length(d.raw_text)
            else 0.0
        end as entity_density,
        coalesce(o.has_overlap, 0) = 1 as has_overlapping_spans,
        coalesce(s.has_high_risk_pii, false) as has_high_risk_pii,
        d.document_key is not null as is_document_key_present,
        d.raw_text is not null and char_length(trim(d.raw_text)) > 0 as is_text_present,
        d.batch_document_key_count = 1 as is_key_unique_within_batch,
        coalesce(s.are_offsets_valid, true) as are_offsets_valid,
        coalesce(s.do_substrings_match, true) as do_substrings_match,
        coalesce(s.are_labels_mapped, true) as are_labels_mapped
    from documents as d
    left join span_checks as s
        on d.batch_id = s.batch_id
        and d.document_key = s.document_key
    left join overlap_checks as o
        on d.batch_id = o.batch_id
        and d.document_key = o.document_key
)

select
    batch_id,
    document_key,
    source_dataset,
    source_format,
    record_id,
    raw_text,
    spans_json,
    language,
    domain,
    template_hash,
    ingested_at,
    text_char_length,
    span_count,
    entity_density,
    has_overlapping_spans,
    has_high_risk_pii,
    is_document_key_present,
    is_text_present,
    is_key_unique_within_batch,
    are_offsets_valid,
    do_substrings_match,
    are_labels_mapped,
    is_document_key_present
        and is_text_present
        and is_key_unique_within_batch
        and are_offsets_valid
        and do_substrings_match
        and are_labels_mapped as dq_passed_flag,
    case
        when not is_document_key_present then 'NULL_DOCUMENT_KEY'
        when not is_key_unique_within_batch then 'DUPLICATE_KEY'
        when not is_text_present then 'NULL_TEXT'
        when not are_offsets_valid then 'OUT_OF_BOUNDS_OFFSET'
        when not do_substrings_match then 'SUBSTRING_MISMATCH'
        when not are_labels_mapped then 'UNMAPPED_ENTITY_LABEL'
    end as failure_reason
from quality_flags
