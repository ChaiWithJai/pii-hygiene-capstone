{{ config(materialized='view') }}

select
    batch_id,
    extraction_key,
    document_key,
    source_dataset,
    upper(source_format) as source_format,
    source_file_uri,
    lower(file_type) as file_type,
    page_number,
    bbox_json,
    ocr_confidence,
    extracted_value,
    raw_entity_label,
    extracted_text,
    nullif(trim(language), '') as language,
    nullif(trim(domain), '') as domain,
    template_hash,
    is_ocr_perturbed,
    ingested_at
from {{ source('raw', 'unstructured_extractions_raw') }}
