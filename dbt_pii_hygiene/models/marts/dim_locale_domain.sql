with locale_domains as (
    select distinct
        coalesce(language, 'unknown') as language,
        coalesce(domain, 'unknown') as domain
    from {{ ref('stg_pii_documents') }}

    union

    select distinct
        coalesce(language, 'unknown') as language,
        coalesce(domain, 'unknown') as domain
    from {{ ref('stg_unstructured_extractions') }}
)

select
    {{ pii_hash("concat_ws('|', language, domain)") }} as locale_domain_key,
    language,
    domain
from locale_domains
