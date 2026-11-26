with structured_failures as (
    select
        {{ pii_hash("concat_ws('|', coalesce(document_key, ''), batch_id, failure_reason, 'structured', cast(row_number() over (partition by batch_id, document_key, failure_reason order by ingested_at, record_id) as text))") }} as quarantine_id,
        document_key,
        batch_id,
        source_dataset,
        failure_reason,
        {{ pii_to_json('q') }} as raw_payload,
        current_timestamp as quarantined_at
    from {{ ref('int_pii_document_quality') }} as q
    where not dq_passed_flag
),

unstructured_failures as (
    select
        {{ pii_hash("concat_ws('|', coalesce(document_key, ''), batch_id, failure_reason, 'unstructured', extraction_key, cast(row_number() over (partition by batch_id, extraction_key, failure_reason order by ingested_at) as text))") }} as quarantine_id,
        document_key,
        batch_id,
        source_dataset,
        failure_reason,
        {{ pii_to_json('q') }} as raw_payload,
        current_timestamp as quarantined_at
    from {{ ref('int_unstructured_quality') }} as q
    where not dq_passed_flag
)

select * from structured_failures
union all
select * from unstructured_failures
