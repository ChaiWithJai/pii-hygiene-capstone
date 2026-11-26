with batch_unique_keys as (
    select batch_id, document_key
    from {{ ref('stg_pii_documents') }}
    group by batch_id, document_key
    having count(*) = 1
)
select q.batch_id, q.document_key, q.failure_reason
from {{ ref('int_pii_document_quality') }} q
inner join batch_unique_keys u
    on u.batch_id = q.batch_id
   and u.document_key = q.document_key
where q.failure_reason = 'DUPLICATE_KEY'
