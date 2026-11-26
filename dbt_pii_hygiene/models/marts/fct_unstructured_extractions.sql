with passing as (
    select
        q.*,
        row_number() over (
            partition by q.extraction_key
            order by q.ingested_at desc, q.batch_id desc
        ) as extraction_rank
    from {{ ref('int_unstructured_quality') }} as q
    where q.dq_passed_flag
)

select
    p.extraction_key,
    p.document_key,
    {{ pii_hash('p.source_dataset') }} as source_dataset_key,
    p.source_file_uri,
    p.file_type,
    p.page_number,
    p.bbox_json,
    p.ocr_confidence,
    p.extracted_value,
    e.entity_type_key,
    p.raw_entity_label,
    e.privacy_tier,
    e.is_high_risk as has_high_risk_pii,
    (p.ocr_confidence < 0.8 and e.is_high_risk) as needs_ocr_verification,
    p.extracted_text,
    p.language,
    p.domain,
    p.batch_id,
    p.ingested_at
from passing as p
left join {{ ref('dim_entity_type') }} as e
    on lower(p.raw_entity_label) = lower(e.raw_entity_label)
where p.extraction_rank = 1
