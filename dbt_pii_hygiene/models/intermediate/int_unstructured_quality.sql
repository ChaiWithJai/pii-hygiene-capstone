{{ config(materialized='table') }}

with mapped as (
    select
        u.*,
        m.canonical_entity_label,
        m.privacy_tier,
        m.is_high_risk,
        coalesce(u.ocr_confidence between 0.0 and 1.0, false)
            as is_ocr_confidence_valid,
        coalesce(u.page_number >= 1, false) as is_page_number_valid,
        coalesce(
            u.source_file_uri is not null
                and char_length(trim(u.source_file_uri)) > 0,
            false
        ) as is_source_uri_present,
        coalesce(m.canonical_entity_label is not null, false) as is_label_mapped
    from {{ ref('stg_unstructured_extractions') }} as u
    left join {{ ref('entity_taxonomy_map') }} as m
        on lower(trim(u.raw_entity_label)) = m.match_key
)

select
    batch_id,
    extraction_key,
    document_key,
    source_dataset,
    source_format,
    source_file_uri,
    file_type,
    page_number,
    bbox_json,
    ocr_confidence,
    extracted_value,
    raw_entity_label,
    canonical_entity_label,
    privacy_tier,
    coalesce(is_high_risk, 0) = 1 as has_high_risk_pii,
    extracted_text,
    language,
    domain,
    template_hash,
    is_ocr_perturbed,
    ingested_at,
    is_ocr_confidence_valid,
    is_page_number_valid,
    is_source_uri_present,
    is_label_mapped,
    is_ocr_confidence_valid
        and is_page_number_valid
        and is_source_uri_present
        and is_label_mapped as dq_passed_flag,
    case
        when not is_ocr_confidence_valid then 'OCR_CONFIDENCE_OUT_OF_RANGE'
        when not is_page_number_valid then 'INVALID_PAGE_NUMBER'
        when not is_source_uri_present then 'MISSING_SOURCE_URI'
        when not is_label_mapped then 'UNMAPPED_ENTITY_LABEL'
    end as failure_reason
from mapped
