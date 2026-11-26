select bridge.document_key, bridge.rule_id
from {{ ref('bridge_record_edge_cases') }} as bridge
where (
    bridge.rule_id = 'LOW_OCR_CONFIDENCE'
    and not exists (
        select 1
        from {{ ref('fct_unstructured_extractions') }} as extraction
        where extraction.document_key = bridge.document_key
          and extraction.needs_ocr_verification
    )
) or (
    bridge.rule_id <> 'LOW_OCR_CONFIDENCE'
    and not exists (
        select 1
        from {{ ref('fct_pii_documents') }} as document
        where document.document_key = bridge.document_key
    )
)
