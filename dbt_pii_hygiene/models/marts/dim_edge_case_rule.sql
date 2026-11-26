select
    rule_id,
    rule_name,
    description,
    severity,
    pattern
from {{ ref('edge_case_rules') }}
