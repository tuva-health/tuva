{{ config(
     enabled = (var('enable_data_quality', false) | string | lower) == 'true'
   )
}}

with structural as (

    select *
    from {{ ref('data_quality__structural_table_status') }}

)

, unpivoted as (

    select
          data_source
        , table_name as input_table_name
        , 'structural__table_exists' as test_name
        , 'table does not exist' as display_name
        , 'Checks whether the expected Input Layer table is missing for the data source.' as description
        , 'table_presence' as test_type
        , table_exists as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , table_name as input_table_name
        , 'structural__table_populated' as test_name
        , 'table is not populated' as display_name
        , 'Checks whether the Input Layer table contains no records for the data source.' as description
        , 'row_population' as test_type
        , table_populated as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , table_name as input_table_name
        , 'structural__columns_exist' as test_name
        , 'columns do not exist' as display_name
        , 'Checks whether one or more expected Input Layer columns are missing.' as description
        , 'column_presence' as test_type
        , columns_exist as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , table_name as input_table_name
        , 'structural__data_types' as test_name
        , 'data types mismatch' as display_name
        , 'Checks whether one or more Input Layer columns use an unexpected data type.' as description
        , 'data_type' as test_type
        , data_types as test_status
        , row_count
    from structural

    union all

    select
          data_source
        , table_name as input_table_name
        , 'structural__primary_keys' as test_name
        , 'primary keys fail' as display_name
        , 'Checks whether the configured Input Layer primary key is null or not unique.' as description
        , 'primary_key' as test_type
        , primary_keys as test_status
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
    , cast(1 as {{ dbt.type_int() }}) as severity
    , cast(coalesce(row_count, 0) as {{ dbt.type_int() }}) as total_row_count
    , cast(case when test_status = 'n/a' then 0 else 1 end as {{ dbt.type_int() }}) as tested_count
    , cast(case when test_status = 'fail' then 1 else 0 end as {{ dbt.type_int() }}) as failed_count
    , cast(case when test_status = 'pass' then 1 else 0 end as {{ dbt.type_int() }}) as passed_count
    , cast(case when test_status = 'n/a' then 1 else 0 end as {{ dbt.type_int() }}) as not_applicable_count
from unpivoted
