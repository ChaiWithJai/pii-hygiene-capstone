# Snowflake port (**UNVERIFIED**)

> **Status: UNVERIFIED.** No Snowflake account, warehouse, database, stage, role,
> dbt run, load, query, or cost/performance test was exercised for this repository.
> The steps below are a target design and must not be presented as deployment proof.

## Target swap

Install `dbt-snowflake`, add a Snowflake output to the dbt profile, and select it
with `--target snowflake`. Preserve the logical database layers and relation names:
RAW, STAGING, INTERMEDIATE, MARTS, QUARANTINE, and GOVERNANCE. Environment variables
or a secret manager must supply account, user/authenticator, role, database,
warehouse, and schema; never commit them.

Map Postgres `JSONB` to Snowflake `VARIANT`, `TIMESTAMPTZ` to `TIMESTAMP_TZ` (or a
documented UTC `TIMESTAMP_NTZ` convention), booleans to `BOOLEAN`, and doubles to
`DOUBLE`. Revalidate nullability and dbt tests because declared constraints and
enforcement differ.

## JSON span expansion

Keep expansion inside an adapter-dispatched macro. The logical output must remain
one row per span with the original zero-based, end-exclusive offsets.

```sql
-- Snowflake shape
select
  f.index::integer as span_index,
  f.value:start::integer as start_offset,
  f.value:end::integer as end_offset,
  f.value:value::string as span_value,
  f.value:label::string as raw_entity_label
from source_table s,
lateral flatten(input => s.spans_json) f
```

BigQuery uses `UNNEST(JSON_QUERY_ARRAY(...))`; Postgres uses
`jsonb_array_elements(...) with ordinality`. Do not put all three dialects directly
inside models. Test empty arrays, null/malformed JSON, Unicode text, substring
alignment, and span ordering on every adapter.

## Proposed loading and storage

Create a private internal or external stage for Parquet plus source PDF/PNG objects.
Load raw tables with explicit column lists and safe conversions. Preserve
`source_file_uri`, page number, bounding box, and confidence. For external stages,
use a storage integration rather than embedded cloud keys. Generated source files
must remain private; provide the app short-lived access rather than public URLs.

## RBAC design

Suggested roles are:

- `PII_INGEST_ROLE`: stage usage and insert/delete-by-batch on RAW only.
- `PII_DBT_TRANSFORM_ROLE`: read RAW and build STAGING through MARTS/QUARANTINE.
- `PII_AGENT_SERVICE_ROLE`: select validated candidate view; insert governance queue
  and action log only.
- `PII_STREAMLIT_READ_ROLE`: select MARTS and governance history.
- `PII_REVIEWER_ROLE`: read permissions plus narrowly scoped review queue updates.

Use separate service users or key-pair/OAuth identities, least-privilege future
grants, network policy, query tags, and short-lived credentials. Snowflake does not
natively grant UPDATE by column; enforce the reviewer's allowed fields through a
stored procedure, service API, or similarly constrained interface rather than broad
table UPDATE in production.

## Validation checklist

The port remains **UNVERIFIED** until a nonproduction Snowflake run demonstrates:

- dbt parse/compile/run/test under the Snowflake target;
- at least 1,000,000 distinct validated document keys at document grain;
- correct span flattening and Unicode substring alignment;
- clean, sample-quarantine, and flood circuit-breaker behavior;
- downstream agent blocking on failed tests and idempotent actions after PASS;
- all four app tabs, artifact traceability, and a constrained human review update;
- trace/lineage continuity, warehouse sizing, elapsed time, and measured cost.
