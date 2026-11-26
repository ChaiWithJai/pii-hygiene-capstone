with passing as (
    select
        q.*,
        row_number() over (
            partition by q.document_key
            order by q.ingested_at desc, q.batch_id desc
        ) as clean_occurrence_rank
    from {{ ref('int_pii_document_quality') }} as q
    where q.dq_passed_flag
),

risk as (
    select *
    from {{ ref('int_document_risk_metrics') }}
)

select
    p.document_key,
    {{ pii_hash('p.source_dataset') }} as source_dataset_key,
    {{ pii_hash("concat_ws('|', coalesce(p.language, 'unknown'), coalesce(p.domain, 'unknown'))") }} as locale_domain_key,
    p.batch_id,
    p.text_char_length,
    p.span_count,
    p.entity_density,
    coalesce(r.has_high_risk_pii, p.has_high_risk_pii, false) as has_high_risk_pii,
    coalesce(r.has_overlapping_spans, p.has_overlapping_spans, false) as has_overlapping_spans,
    p.dq_passed_flag,
    p.ingested_at,
    p.source_dataset,
    p.source_format,
    p.language,
    p.domain,
    p.template_hash
from passing as p
left join risk as r
    on p.document_key = r.document_key
    and p.batch_id = r.batch_id
where p.clean_occurrence_rank = 1
