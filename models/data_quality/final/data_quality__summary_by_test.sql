{{ config(
     enabled = var('enable_data_quality', false) | as_bool
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
    from {{ ref('data_quality__catalog_domain_requirement') }} as requirements
    inner join {{ ref('data_quality__logical_test_result') }} as logical_results
        on requirements.input_table_name = logical_results.input_table_name
    where requirements.requirement_level = 'required'
      and (
          requirements.input_column_name is null
          or exists (
              select 1
              from {{ ref('int_data_quality__catalog_logical_test_column') }} as test_columns
              where logical_results.test_name = test_columns.test_name
                and logical_results.input_table_name = test_columns.input_table_name
                and requirements.input_column_name = test_columns.input_column_name
          )
      )

)

, structural_component_tests as (

    select distinct
          requirements.domain_group_key
        , requirements.component_key
        , structural_results.data_source
        , structural_results.input_table_name
        , structural_results.test_name
        , structural_results.display_name
        , structural_results.description
        , structural_results.grain
        , structural_results.test_type
        , structural_results.check_category
        , structural_results.severity
        , structural_results.total_row_count
        , structural_results.tested_count
        , structural_results.failed_count
        , structural_results.passed_count
        , structural_results.not_applicable_count
        , 'structural' as test_source
    from {{ ref('data_quality__catalog_domain_requirement') }} as requirements
    inner join {{ ref('data_quality__structural_test_result') }} as structural_results
        on requirements.input_table_name = structural_results.input_table_name
    where requirements.requirement_level = 'required'

)

, unioned as (

    select * from logical_component_tests

    union all

    select * from structural_component_tests

)

select
      unioned.data_source
    , catalog.domain_group_key
    , catalog.domain_group_name
    , catalog.component_key
    , catalog.component_name
    , unioned.input_table_name
    , unioned.test_name
    , unioned.display_name
    , unioned.description
    , unioned.grain
    , unioned.test_type
    , unioned.check_category
    , unioned.severity
    , unioned.test_source
    , cast(sum(coalesce(unioned.total_row_count, 0)) as {{ dbt.type_int() }}) as total_row_count
    , cast(sum(coalesce(unioned.tested_count, 0)) as {{ dbt.type_int() }}) as tested_count
    , cast(sum(coalesce(unioned.passed_count, 0)) as {{ dbt.type_int() }}) as passed_count
    , cast(sum(coalesce(unioned.failed_count, 0)) as {{ dbt.type_int() }}) as failed_count
    , cast(sum(coalesce(unioned.not_applicable_count, 0)) as {{ dbt.type_int() }}) as not_applicable_count
from unioned
inner join {{ ref('data_quality__catalog_domain') }} as catalog
    on unioned.domain_group_key = catalog.domain_group_key
   and unioned.component_key = catalog.component_key
group by
      unioned.data_source
    , catalog.domain_group_key
    , catalog.domain_group_name
    , catalog.component_key
    , catalog.component_name
    , unioned.input_table_name
    , unioned.test_name
    , unioned.display_name
    , unioned.description
    , unioned.grain
    , unioned.test_type
    , unioned.check_category
    , unioned.severity
    , unioned.test_source
