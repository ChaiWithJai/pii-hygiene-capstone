{% macro pii_hash(expression) -%}
    {# A lowercase, 64-character SHA-256 hex digest on every supported adapter. #}
    {%- if target.type == 'postgres' -%}
        encode(digest(cast({{ expression }} as text), 'sha256'), 'hex')
    {%- elif target.type == 'snowflake' -%}
        lower(sha2(cast({{ expression }} as varchar), 256))
    {%- elif target.type == 'bigquery' -%}
        lower(to_hex(sha256(cast({{ expression }} as bytes))))
    {%- else -%}
        {{ exceptions.raise_compiler_error(
            "pii_hash is not implemented for adapter " ~ target.type
        ) }}
    {%- endif -%}
{%- endmacro %}
