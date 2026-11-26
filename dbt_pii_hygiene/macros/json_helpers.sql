{% macro pii_json_object(pairs) -%}
    {# `pairs` is a Jinja list of (literal key, SQL value expression) tuples. #}
    {%- if target.type == 'postgres' -%}
        jsonb_build_object(
        {%- for key, value in pairs -%}
            '{{ key }}', {{ value }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {%- elif target.type == 'snowflake' -%}
        object_construct_keep_null(
        {%- for key, value in pairs -%}
            '{{ key }}', {{ value }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {%- elif target.type == 'bigquery' -%}
        json_object(
        {%- for key, value in pairs -%}
            '{{ key }}', {{ value }}{% if not loop.last %}, {% endif %}
        {%- endfor -%}
        )
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "pii_json_object is not implemented for adapter " ~ target.type
        ) }}
    {%- endif -%}
{%- endmacro %}

{% macro pii_to_json(expression) -%}
    {%- if target.type == 'postgres' -%}
        to_jsonb({{ expression }})
    {%- elif target.type == 'snowflake' -%}
        to_variant({{ expression }})
    {%- elif target.type == 'bigquery' -%}
        to_json({{ expression }})
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "pii_to_json is not implemented for adapter " ~ target.type
        ) }}
    {%- endif -%}
{%- endmacro %}

{% macro pii_json_text(expression, key) -%}
    {# Extract a top-level JSON scalar as warehouse text. `key` is a literal key. #}
    {%- if target.type == 'postgres' -%}
        ({{ expression }} ->> '{{ key }}')
    {%- elif target.type == 'snowflake' -%}
        ({{ expression }}:{{ adapter.quote(key) }}::varchar)
    {%- elif target.type == 'bigquery' -%}
        json_value({{ expression }}, '$.{{ key }}')
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "pii_json_text is not implemented for adapter " ~ target.type
        ) }}
    {%- endif -%}
{%- endmacro %}
