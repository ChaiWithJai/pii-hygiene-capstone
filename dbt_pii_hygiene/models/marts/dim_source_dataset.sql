with sources as (
    select distinct source_dataset, source_format
    from {{ ref('stg_pii_documents') }}
    where source_dataset is not null

    union

    select distinct source_dataset, source_format
    from {{ ref('stg_unstructured_extractions') }}
    where source_dataset is not null
)

select
    {{ pii_hash('source_dataset') }} as source_dataset_key,
    source_dataset,
    source_format
from sources
