{{ config(
     enabled = the_tuva_project.tuva_boolean_var('data_quality_enabled', false),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{% set current_date_sql = dq_current_date_sql() %}
{% set tomorrow_sql = dbt.dateadd(
    datepart='day',
    interval=1,
    from_date_or_timestamp=current_date_sql
  ) %}

with test_cases as (
    select
          'null_value' as test_case
        , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
        , cast(null as {{ dbt.type_int() }}) as expected_flag
    union all
    select 'before_minimum', cast('1999-12-31' as {{ dbt.type_timestamp() }}), 1
    union all
    select 'at_minimum', cast('2000-01-01' as {{ dbt.type_timestamp() }}), 0
    union all
    select 'at_current_date', cast({{ current_date_sql }} as {{ dbt.type_timestamp() }}), 0
    union all
    select 'tomorrow', cast({{ tomorrow_sql }} as {{ dbt.type_timestamp() }}), 1
)

, actual_results as (
    select
          test_case
        , expected_flag
        , {{ dq_logical_ingest_datetime_range_flag_sql('ingest_datetime') }} as actual_flag
    from test_cases
)

select *
from actual_results
where coalesce(actual_flag, -1) <> coalesce(expected_flag, -1)
