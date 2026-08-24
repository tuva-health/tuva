{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

with test_cases as (
    select
          cast(null as {{ api.Column.translate_type('date') }}) as date_value
        , cast(null as {{ dbt.type_int() }}) as expected_flag
    union all
    select {{ dq_date_literal_sql('1899-12-31') }}, cast(1 as {{ dbt.type_int() }})
    union all
    select {{ dq_date_literal_sql('1900-01-01') }}, cast(0 as {{ dbt.type_int() }})
    union all
    select {{ dq_date_literal_sql('2100-12-31') }}, cast(0 as {{ dbt.type_int() }})
    union all
    select {{ dq_date_literal_sql('2101-01-01') }}, cast(1 as {{ dbt.type_int() }})
)

, actual_flags as (
    select
          date_value
        , expected_flag
        , {{ dq_logical_supported_date_range_flag_sql('date_value') }} as actual_flag
    from test_cases
)

select *
from actual_flags
where actual_flag <> expected_flag
   or (actual_flag is null and expected_flag is not null)
   or (actual_flag is not null and expected_flag is null)
