{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'component_test_quality_results',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

with logical_component_tests as (

    select distinct
          requirements.domain_group_key
        , requirements.component_key
        , logical_results.data_source
        , logical_results.input_table_name
        , logical_results.test_name
        , logical_results.display_name
        , logical_results.description
        , logical_results.grain
        , logical_results.test_type
        , logical_results.check_category
        , logical_results.severity
        , logical_results.total_row_count
        , logical_results.tested_count
        , logical_results.failed_count
        , logical_results.passed_count
        , logical_results.not_applicable_count
        , 'logical' as test_source
    from {{ ref('data_quality__domain_input_requirements') }} as requirements
    inner join {{ ref('data_quality__logical_test_results') }} as logical_results
        on requirements.input_table_name = logical_results.input_table_name
    where requirements.requirement_level = 'required'
      and (
          requirements.input_column_name is null
          or exists (
              select 1
              from {{ ref('data_quality__logical_test_input_columns') }} as test_columns
              where logical_results.test_name = test_columns.test_name
                and logical_results.input_table_name = test_columns.input_table_name
                and requirements.input_column_name = test_columns.input_column_name
          )
      )

)

select
      logical_component_tests.data_source
    , catalog.domain_group_key
    , catalog.domain_group_name
    , catalog.component_key
    , catalog.component_name
    , logical_component_tests.input_table_name
    , logical_component_tests.test_name
    , logical_component_tests.display_name
    , logical_component_tests.description
    , logical_component_tests.grain
    , logical_component_tests.test_type
    , logical_component_tests.check_category
    , logical_component_tests.severity
    , logical_component_tests.test_source
    , cast(sum(coalesce(logical_component_tests.total_row_count, 0)) as {{ dbt.type_int() }}) as total_row_count
    , cast(sum(coalesce(logical_component_tests.tested_count, 0)) as {{ dbt.type_int() }}) as tested_count
    , cast(sum(coalesce(logical_component_tests.passed_count, 0)) as {{ dbt.type_int() }}) as passed_count
    , cast(sum(coalesce(logical_component_tests.failed_count, 0)) as {{ dbt.type_int() }}) as failed_count
    , cast(sum(coalesce(logical_component_tests.not_applicable_count, 0)) as {{ dbt.type_int() }}) as not_applicable_count
from logical_component_tests
inner join {{ ref('data_quality__domain_catalog') }} as catalog
    on logical_component_tests.domain_group_key = catalog.domain_group_key
   and logical_component_tests.component_key = catalog.component_key
group by
      logical_component_tests.data_source
    , catalog.domain_group_key
    , catalog.domain_group_name
    , catalog.component_key
    , catalog.component_name
    , logical_component_tests.input_table_name
    , logical_component_tests.test_name
    , logical_component_tests.display_name
    , logical_component_tests.description
    , logical_component_tests.grain
    , logical_component_tests.test_type
    , logical_component_tests.check_category
    , logical_component_tests.severity
    , logical_component_tests.test_source
