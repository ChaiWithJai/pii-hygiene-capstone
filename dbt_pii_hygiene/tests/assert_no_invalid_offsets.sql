select *
from {{ ref('fct_pii_spans') }}
where not offset_valid_flag
