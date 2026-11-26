{{ config(materialized='view') }}

select * from {{ ref('fct_pii_documents') }}
