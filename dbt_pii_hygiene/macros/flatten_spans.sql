{% macro flatten_spans(json_expression, json_column=none) -%}
    {#
      Return a complete CROSS JOIN fragment with a stable `flattened` record.

      Preferred call:
        from source_rows
        {{ flatten_spans('spans_json') }}

      The two-argument form is retained for readable qualified references:
        {{ flatten_spans('d', 'spans_json') }}

      Output fields are span_index (zero based), start_offset, end_offset,
      span_value, and raw_entity_label.
    #}
    {%- if json_column is not none -%}
        {%- set json_expression = json_expression ~ '.' ~ json_column -%}
    {%- endif -%}
    {%- if target.type == 'postgres' -%}
cross join lateral (
    select
        (span_item.ordinality - 1)::bigint as span_index,
        case
            when pg_input_is_valid(span_item.value ->> 'start', 'integer')
                then (span_item.value ->> 'start')::integer
        end as start_offset,
        case
            when pg_input_is_valid(span_item.value ->> 'end', 'integer')
                then (span_item.value ->> 'end')::integer
        end as end_offset,
        span_item.value ->> 'value' as span_value,
        span_item.value ->> 'label' as raw_entity_label
    from jsonb_array_elements(
        coalesce({{ json_expression }}, '[]'::jsonb)
    ) with ordinality as span_item(value, ordinality)
) as flattened
    {%- elif target.type == 'snowflake' -%}
cross join lateral (
    select
        span_item.index::integer as span_index,
        try_to_number(span_item.value:start::string)::integer as start_offset,
        try_to_number(span_item.value:end::string)::integer as end_offset,
        span_item.value:value::string as span_value,
        span_item.value:label::string as raw_entity_label
    from table(flatten(input => {{ json_expression }})) as span_item
) as flattened
    {%- elif target.type == 'bigquery' -%}
cross join unnest(array(
    select as struct
        span_index,
        safe_cast(json_value(span_item, '$.start') as int64) as start_offset,
        safe_cast(json_value(span_item, '$.end') as int64) as end_offset,
        json_value(span_item, '$.value') as span_value,
        json_value(span_item, '$.label') as raw_entity_label
    from unnest(ifnull(json_query_array({{ json_expression }}), [])) as span_item
    with offset as span_index
)) as flattened
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "flatten_spans is not implemented for adapter " ~ target.type
        ) }}
    {%- endif -%}
{%- endmacro %}
