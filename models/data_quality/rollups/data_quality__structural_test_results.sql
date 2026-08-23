{{ config(
     enabled = var('data_quality_enabled', false) | as_bool,
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'structural_test_results',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup', 'dq_structural'],
     materialized = 'table'
   )
}}

with structural as (

    select *
    from {{ ref('data_quality__structural') }}

)

, unpivoted as (

    select
          data_source
        , input_table_name
        , 'structural__table_populated' as test_name
        , 'table is not populated' as display_name
        , 'Checks whether the Warehouse Table or View contains records for the data source.' as description
        , 'row_population' as test_type
        , table_populated as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , input_table_name
        , 'structural__columns_exist' as test_name
        , 'columns do not exist' as display_name
        , 'Checks whether one or more columns required by the Input Layer contract are missing.' as description
        , 'column_presence' as test_type
        , columns_exist as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , input_table_name
        , 'structural__data_types_correct' as test_name
        , 'data types mismatch' as display_name
        , 'Checks whether one or more Input Layer columns use an unexpected data type.' as description
        , 'data_type' as test_type
        , data_types_correct as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , input_table_name
        , 'structural__primary_key_correct' as test_name
        , 'primary keys fail' as display_name
        , 'Checks whether the configured Input Layer primary key is null or not unique.' as description
        , 'primary_key' as test_type
        , primary_key_correct as test_status
        , row_count
    from structural

)

select
      data_source
    , input_table_name
    , test_name
    , display_name
    , description
    , 'structural' as grain
    , test_type
    , 'structural' as check_category
    , cast(case when test_status = 'fail' then 1 else null end as {{ dbt.type_int() }}) as severity
    , cast(coalesce(row_count, 0) as {{ dbt.type_bigint() }}) as total_row_count
    , cast(case when test_status = 'not evaluated' then 0 else 1 end as {{ dbt.type_int() }}) as tested_count
    , cast(case when test_status = 'fail' then 1 else 0 end as {{ dbt.type_int() }}) as failed_count
    , cast(case when test_status = 'pass' then 1 else 0 end as {{ dbt.type_int() }}) as passed_count
    , cast(case when test_status = 'not evaluated' then 1 else 0 end as {{ dbt.type_int() }}) as not_evaluated_count
from unpivoted
