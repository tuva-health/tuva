{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{% set date_type = api.Column.translate_type('date') %}
{% set current_date_sql = dq_current_date_sql() %}
{% set one_year_after_current_date_sql = "cast(" ~ dbt.dateadd(
    datepart='month',
    interval=12,
    from_date_or_timestamp=current_date_sql
  ) ~ " as " ~ date_type ~ ")" %}
{% set one_day_after_one_year_sql = "cast(" ~ dbt.dateadd(
    datepart='day',
    interval=1,
    from_date_or_timestamp=one_year_after_current_date_sql
  ) ~ " as " ~ date_type ~ ")" %}

with test_cases as (
    select
          'null_value' as test_case
        , cast(null as {{ date_type }}) as date_value
        , {{ dq_date_literal_sql('1900-01-01') }} as minimum_date_value
        , {{ current_date_sql }} as maximum_date_value
        , cast(null as {{ dbt.type_int() }}) as expected_in_range
        , cast(null as {{ dbt.type_int() }}) as expected_flag
        , cast(null as {{ dbt.type_int() }}) as expected_spine_in_range
        , cast(null as {{ dbt.type_int() }}) as expected_ingest_flag
    union all
    select 'before_minimum', {{ dq_date_literal_sql('1899-12-31') }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 0, 1, 0, 1
    union all
    select 'at_minimum', {{ dq_date_literal_sql('1900-01-01') }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 1, 0, 1, 1
    union all
    select 'at_ingest_minimum', {{ dq_date_literal_sql('2000-01-01') }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 1, 0, 1, 0
    union all
    select 'at_current_date', {{ current_date_sql }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 1, 0, 1, 0
    union all
    select 'at_dynamic_maximum', {{ one_year_after_current_date_sql }}, {{ dq_date_literal_sql('2000-01-01') }}, {{ one_year_after_current_date_sql }}, 1, 0, 1, 1
    union all
    select 'after_dynamic_maximum', {{ one_day_after_one_year_sql }}, {{ dq_date_literal_sql('2000-01-01') }}, {{ one_year_after_current_date_sql }}, 0, 1, 1, 1
    union all
    select 'at_spine_maximum', {{ dq_date_literal_sql('2100-12-31') }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 0, 1, 1, 1
    union all
    select 'after_spine_maximum', {{ dq_date_literal_sql('2101-01-01') }}, {{ dq_date_literal_sql('1900-01-01') }}, {{ current_date_sql }}, 0, 1, 0, 1
)

, actual_results as (
    select
          test_case
        , expected_in_range
        , expected_flag
        , expected_spine_in_range
        , expected_ingest_flag
        , cast(case
            when {{ dq_date_in_range_where_sql('date_value', 'minimum_date_value', 'maximum_date_value') }} then 1
            when not {{ dq_date_in_range_where_sql('date_value', 'minimum_date_value', 'maximum_date_value') }} then 0
          end as {{ dbt.type_int() }}) as actual_in_range
        , {{ dq_logical_date_range_flag_sql(
            'date_value',
            'minimum_date_value',
            'maximum_date_value'
          ) }} as actual_flag
        , cast(case
            when {{ dq_member_month_spine_date_where_sql('date_value') }} then 1
            when not {{ dq_member_month_spine_date_where_sql('date_value') }} then 0
          end as {{ dbt.type_int() }}) as actual_spine_in_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
            'cast(date_value as ' ~ api.Column.translate_type('timestamp') ~ ')'
          ) }} as actual_ingest_flag
    from test_cases
)

select *
from actual_results
where coalesce(actual_in_range, -1) <> coalesce(expected_in_range, -1)
   or coalesce(actual_flag, -1) <> coalesce(expected_flag, -1)
   or coalesce(actual_spine_in_range, -1) <> coalesce(expected_spine_in_range, -1)
   or coalesce(actual_ingest_flag, -1) <> coalesce(expected_ingest_flag, -1)
