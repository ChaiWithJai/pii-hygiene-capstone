{{ config(materialized='view') }}

with ocr_verification as (
    select
        {{ pii_hash("concat_ws('|', e.extraction_key, 'OCR_VERIFICATION')") }} as candidate_key,
        'OCR_VERIFICATION' as candidate_type,
        e.document_key,
        e.source_file_uri,
        e.batch_id,
        'HIGH' as priority,
        d.canonical_entity_label as entity_type,
        e.ocr_confidence,
        cast(null as {{ dbt.type_float() }}) as entity_density,
        cast(false as boolean) as has_overlapping_spans,
        cast(null as {{ dbt.type_float() }}) as quarantine_rate,
        {{ pii_json_object([
            ('extraction_key', 'e.extraction_key'),
            ('page_number', 'e.page_number'),
            ('bbox_json', 'e.bbox_json'),
            ('privacy_tier', 'e.privacy_tier')
        ]) }} as evidence_json
    from {{ ref('fct_unstructured_extractions') }} as e
    left join {{ ref('dim_entity_type') }} as d using (entity_type_key)
    where e.has_high_risk_pii and e.ocr_confidence < 0.8
),

redaction_review as (
    select
        {{ pii_hash("concat_ws('|', document_key, batch_id, 'REDACTION_REVIEW')") }} as candidate_key,
        'REDACTION_REVIEW' as candidate_type,
        document_key,
        cast(null as text) as source_file_uri,
        batch_id,
        'HIGH' as priority,
        cast(null as text) as entity_type,
        cast(null as {{ dbt.type_float() }}) as ocr_confidence,
        entity_density,
        has_overlapping_spans,
        cast(null as {{ dbt.type_float() }}) as quarantine_rate,
        {{ pii_json_object([
            ('span_count', 'span_count'),
            ('entity_density', 'entity_density'),
            ('has_overlapping_spans', 'has_overlapping_spans'),
            ('has_high_risk_pii', 'has_high_risk_pii')
        ]) }} as evidence_json
    from {{ ref('fct_pii_documents') }}
    where has_high_risk_pii
      and (has_overlapping_spans or entity_density > 2.5)
),

batch_totals as (
    select batch_id, count(*) as total_count
    from (
        select batch_id from {{ ref('stg_pii_documents') }}
        union all
        select batch_id from {{ ref('stg_unstructured_extractions') }}
    ) as landed
    group by batch_id
),

quarantine_counts as (
    select batch_id, count(*) as quarantine_count
    from {{ ref('pii_quarantined_documents') }}
    group by batch_id
),

quarantine_entities as (
    select
        batch_id,
        coalesce({{ pii_json_text('raw_payload', 'raw_entity_label') }}, 'UNKNOWN') as entity_type,
        count(*) as entity_count,
        row_number() over (
            partition by batch_id
            order by count(*) desc, coalesce({{ pii_json_text('raw_payload', 'raw_entity_label') }}, 'UNKNOWN')
        ) as entity_rank
    from {{ ref('pii_quarantined_documents') }}
    group by batch_id, coalesce({{ pii_json_text('raw_payload', 'raw_entity_label') }}, 'UNKNOWN')
),

slice_quarantine_alert as (
    select
        {{ pii_hash("concat_ws('|', t.batch_id, e.entity_type, 'SLICE_QUARANTINE_ALERT')") }} as candidate_key,
        'SLICE_QUARANTINE_ALERT' as candidate_type,
        concat('batch:', t.batch_id) as document_key,
        cast(null as text) as source_file_uri,
        t.batch_id,
        'MEDIUM' as priority,
        e.entity_type,
        cast(null as {{ dbt.type_float() }}) as ocr_confidence,
        cast(null as {{ dbt.type_float() }}) as entity_density,
        cast(false as boolean) as has_overlapping_spans,
        cast(q.quarantine_count as {{ dbt.type_float() }}) / nullif(t.total_count, 0) as quarantine_rate,
        {{ pii_json_object([
            ('quarantined_rows', 'q.quarantine_count'),
            ('landed_rows', 't.total_count'),
            ('entity_quarantined_rows', 'e.entity_count'),
            ('entity_concentration', 'cast(e.entity_count as ' ~ dbt.type_float() ~ ') / nullif(q.quarantine_count, 0)')
        ]) }} as evidence_json
    from batch_totals as t
    inner join quarantine_counts as q using (batch_id)
    inner join quarantine_entities as e using (batch_id)
    where e.entity_rank = 1
      and cast(e.entity_count as {{ dbt.type_float() }}) / nullif(q.quarantine_count, 0) > 0.5
      and cast(q.quarantine_count as {{ dbt.type_float() }}) / nullif(t.total_count, 0) between 0.01 and 0.05
),

verified_false_positive as (
    select
        {{ pii_hash("concat_ws('|', e.extraction_key, 'VERIFIED_FALSE_POSITIVE')") }} as candidate_key,
        'VERIFIED_FALSE_POSITIVE' as candidate_type,
        e.document_key,
        e.source_file_uri,
        e.batch_id,
        'LOW' as priority,
        d.canonical_entity_label as entity_type,
        e.ocr_confidence,
        cast(null as {{ dbt.type_float() }}) as entity_density,
        cast(false as boolean) as has_overlapping_spans,
        cast(null as {{ dbt.type_float() }}) as quarantine_rate,
        {{ pii_json_object([
            ('extraction_key', 'e.extraction_key'),
            ('privacy_tier', 'e.privacy_tier'),
            ('rationale', "'clean low-risk extraction with high OCR confidence'")
        ]) }} as evidence_json
    from {{ ref('fct_unstructured_extractions') }} as e
    left join {{ ref('dim_entity_type') }} as d using (entity_type_key)
    where not e.has_high_risk_pii and e.ocr_confidence >= 0.9
)

select * from ocr_verification
union all
select * from redaction_review
union all
select * from slice_quarantine_alert
union all
select * from verified_false_positive
