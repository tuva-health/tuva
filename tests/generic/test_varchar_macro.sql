
-- ============================================================================
-- Unit tests for varchar macros
-- Framework: dbt Core data tests (SQL with Jinja). These tests return 0 rows on success.
-- Notes:
--  - Validates adapter-dispatched varchar() output against adapter-specific expectations.
--  - Validates specific implementations: default__varchar and databricks__varchar.
--  - Adds invariant checks: non-empty, trimmed, uppercase.
-- ============================================================================

{# Compute expected results for current adapter #}
{% set expected_default = default__varchar() %}
{% set expected_databricks = databricks__varchar() %}
{% set expected_dispatch = expected_default %}
{% if target.type == 'databricks' %}
  {% set expected_dispatch = expected_databricks %}
{% endif %}
{% set dispatched = varchar() %}

with checks as (
    -- Specific macro implementations
    select 'default__varchar returns VARCHAR' as check_name,
           case when '{{ expected_default }}' = 'VARCHAR' then 0 else 1 end as failure
    union all
    select 'databricks__varchar returns VARCHAR(255)' as check_name,
           case when '{{ expected_databricks }}' = 'VARCHAR(255)' then 0 else 1 end as failure

    -- Dispatch behavior for current adapter
    union all
    select 'varchar() dispatch returns correct for adapter' as check_name,
           case when '{{ dispatched }}' = '{{ expected_dispatch }}' then 0 else 1 end as failure

    -- Invariants on the dispatched result
    union all
    select 'varchar() output is non-empty' as check_name,
           case when length('{{ dispatched }}') > 0 then 0 else 1 end as failure
    union all
    select 'varchar() output has no leading/trailing spaces' as check_name,
           case when '{{ dispatched }}' = trim('{{ dispatched }}') then 0 else 1 end as failure
    union all
    select 'varchar() output is uppercase' as check_name,
           case when upper('{{ dispatched }}') = '{{ dispatched }}' then 0 else 1 end as failure
)
select check_name, failure
from checks
where failure = 1
;
