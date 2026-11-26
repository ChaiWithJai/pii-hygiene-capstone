-- A key may legitimately occur in a clean rerun batch.  Only the quarantined
-- occurrence, identified by both key and batch, must be excluded from marts.
select q.document_key, q.batch_id
from {{ ref('pii_quarantined_documents') }} q
inner join {{ ref('fct_pii_documents') }} d
    on d.document_key = q.document_key
   and d.batch_id = q.batch_id
