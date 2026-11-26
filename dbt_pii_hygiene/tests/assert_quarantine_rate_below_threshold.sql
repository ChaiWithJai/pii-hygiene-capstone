with batch_totals as (
    select batch_id, count(*) as total_count
    from {{ source('raw', 'pii_documents_raw') }}
    group by batch_id
),
quarantined as (
    select batch_id, count(*) as quarantined_count
    from {{ ref('pii_quarantined_documents') }}
    group by batch_id
)
select
    t.batch_id,
    coalesce(q.quarantined_count, 0) as quarantined_count,
    t.total_count,
    coalesce(q.quarantined_count, 0) * 1.0 / nullif(t.total_count, 0) as quarantine_rate
from batch_totals t
left join quarantined q using (batch_id)
where coalesce(q.quarantined_count, 0) * 1.0 / nullif(t.total_count, 0)
    > {{ var('quarantine_threshold', 0.05) }}
