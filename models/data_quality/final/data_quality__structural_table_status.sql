{{ config(
     enabled = var('enable_data_quality', false) | as_bool
   )
}}

with column_status as (

    select
          data_source
        , {{ adapter.quote('table') }} as table_name
        , max(table_exists) as table_exists
        , cast(max(row_count) as {{ dbt.type_int() }}) as row_count
        , cast(sum(case when column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_column_count
        , cast(sum(case when data_type_correct = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as bad_data_type_count
        , cast(sum(case when is_primary_key = 'yes' then 1 else 0 end) as {{ dbt.type_int() }}) as expected_pk_column_count
        , cast(sum(case when is_primary_key = 'yes' and column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_pk_column_count
    from {{ ref('int_data_quality__structural_column_comparison') }}
    group by
          data_source
        , {{ adapter.quote('table') }}

)

, pk_test_status as (

    select
          data_source
        , {{ adapter.quote('table') }} as table_name
        , cast(sum(case when test_result is null or test_result <> 0 then 1 else 0 end) as {{ dbt.type_int() }}) as failing_test_count
    from {{ ref('int_data_quality__structural_primary_key_test') }}
    group by
          data_source
        , {{ adapter.quote('table') }}

)

select
      column_status.data_source
    , column_status.table_name
    , case
        when column_status.table_exists = 'yes' then 'pass'
        else 'fail'
      end as table_exists
    , case
        when column_status.table_exists = 'yes'
         and coalesce(column_status.row_count, 0) > 0
        then 'pass'
        else 'fail'
      end as table_populated
    , case
        when column_status.missing_column_count = 0 then 'pass'
        else 'fail'
      end as columns_exist
    , case
        when column_status.bad_data_type_count = 0 then 'pass'
        else 'fail'
      end as data_types
    , case
        when column_status.table_exists = 'yes'
         and coalesce(column_status.row_count, 0) = 0
        then 'n/a'
        when column_status.table_exists = 'yes'
         and column_status.expected_pk_column_count > 0
         and column_status.missing_pk_column_count = 0
         and coalesce(pk_test_status.failing_test_count, 0) = 0
        then 'pass'
        else 'fail'
      end as primary_keys
    , column_status.row_count
from column_status
left outer join pk_test_status
    on column_status.table_name = pk_test_status.table_name
    and (
        column_status.data_source = pk_test_status.data_source
        or (
            column_status.data_source is null
            and pk_test_status.data_source is null
        )
    )
