with duplicated_keys as (
    select batch_id, document_key
    from {{ ref('stg_pii_documents') }}
    group by batch_id, document_key
    having count(*) > 1
)
select q.batch_id, q.document_key, q.dq_passed_flag, q.failure_reason
from {{ ref('int_pii_document_quality') }} q
inner join duplicated_keys d
    on d.batch_id = q.batch_id
   and d.document_key = q.document_key
where q.dq_passed_flag
   or q.failure_reason <> 'DUPLICATE_KEY'
