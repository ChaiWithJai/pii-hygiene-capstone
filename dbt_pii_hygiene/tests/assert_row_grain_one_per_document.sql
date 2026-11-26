select document_key, count(*) as row_count
from {{ ref('fct_pii_documents') }}
group by document_key
having count(*) > 1
