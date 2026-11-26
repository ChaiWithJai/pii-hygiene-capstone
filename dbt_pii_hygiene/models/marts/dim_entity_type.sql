select
    {{ pii_hash('match_key') }} as entity_type_key,
    raw_entity_label,
    canonical_entity_label,
    privacy_tier,
    cast(is_high_risk as integer) = 1 as is_high_risk,
    description
from {{ ref('entity_taxonomy_map') }}
