{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural',
     tags = ['data_quality', 'dq_structural'],
     materialized = 'table'
   )
}}

with column_status as (

    select
          input_table_name
        , cast(sum(case when column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_column_count
        , cast(sum(case when data_type_correct = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as bad_data_type_count
        , cast(sum(case when is_primary_key = 'yes' then 1 else 0 end) as {{ dbt.type_int() }}) as expected_pk_column_count
        , cast(sum(case when is_primary_key = 'yes' and column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_pk_column_count
        , cast(sum(case when column_name = 'data_source' and column_exists = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as missing_data_source_column_count
        , cast(sum(case when column_name = 'data_source' and column_exists = 'yes' and data_type_correct = 'no' then 1 else 0 end) as {{ dbt.type_int() }}) as bad_data_source_type_count
    from {{ ref('data_quality__structural_column_details') }}
    group by
          input_table_name

)

, evaluation_scope as (

    select
          data_source
        , input_table_name
        , cast(row_count as {{ dbt.type_bigint() }}) as row_count
    from {{ ref('data_quality__structural_evaluation_scope') }}

)

, pk_test_status as (

    select
          data_source
        , input_table_name
        , cast(count(*) as {{ dbt.type_int() }}) as test_count
        , cast(sum(case when test_result is null then 1 else 0 end) as {{ dbt.type_int() }}) as not_evaluated_test_count
        , cast(sum(case when test_result <> 0 then 1 else 0 end) as {{ dbt.type_int() }}) as failing_test_count
    from {{ ref('data_quality__structural_primary_key_tests') }}
    group by
          data_source
        , input_table_name

)

select
      evaluation_scope.data_source
    , evaluation_scope.input_table_name
    , case
        when column_status.missing_column_count = 0 then 'pass'
        else 'fail'
      end as columns_exist
    , case
        when column_status.missing_column_count > 0 then 'not evaluated'
        when column_status.bad_data_type_count = 0 then 'pass'
        else 'fail'
      end as data_types_correct
    , case
        when column_status.missing_data_source_column_count > 0
          or column_status.bad_data_source_type_count > 0
          or evaluation_scope.row_count is null
        then 'not evaluated'
        when evaluation_scope.row_count > 0 then 'pass'
        else 'fail'
      end as table_populated
    , case
        when column_status.missing_data_source_column_count > 0
          or column_status.bad_data_source_type_count > 0
          or evaluation_scope.row_count is null
          or evaluation_scope.row_count = 0
          or column_status.expected_pk_column_count = 0
          or column_status.missing_pk_column_count > 0
          or coalesce(pk_test_status.test_count, 0) = 0
          or coalesce(pk_test_status.not_evaluated_test_count, 0) > 0
        then 'not evaluated'
        when coalesce(pk_test_status.failing_test_count, 0) = 0
        then 'pass'
        else 'fail'
      end as primary_key_correct
    , evaluation_scope.row_count
from evaluation_scope
inner join column_status
    on evaluation_scope.input_table_name = column_status.input_table_name
left outer join pk_test_status
    on evaluation_scope.input_table_name = pk_test_status.input_table_name
    and (
        evaluation_scope.data_source = pk_test_status.data_source
        or (
            evaluation_scope.data_source is null
            and pk_test_status.data_source is null
        )
    )
