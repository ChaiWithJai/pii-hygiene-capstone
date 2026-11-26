select document_key, raw_entity_label
from {{ ref('fct_pii_spans') }}
where entity_type_key is null

union all

select document_key, raw_entity_label
from {{ ref('fct_unstructured_extractions') }}
where entity_type_key is null
