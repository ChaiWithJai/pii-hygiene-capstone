with document_rules as (
    select document_key, 'HIGH_RISK_PII' as rule_id
    from {{ ref('fct_pii_documents') }}
    where has_high_risk_pii

    union all

    select document_key, 'OVERLAPPING_SPANS' as rule_id
    from {{ ref('fct_pii_documents') }}
    where has_overlapping_spans

    union all

    select document_key, 'HIGH_ENTITY_DENSITY' as rule_id
    from {{ ref('fct_pii_documents') }}
    where entity_density > 2.5

    union all

    select distinct document_key, 'LOW_OCR_CONFIDENCE' as rule_id
    from {{ ref('fct_unstructured_extractions') }}
    where needs_ocr_verification
)

select distinct r.document_key, r.rule_id
from document_rules as r
inner join {{ ref('dim_edge_case_rule') }} as d using (rule_id)
