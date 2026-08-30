{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

with catalog_tests as (
    select
          test_name
        , input_table_name
    from {{ ref('data_quality__logical_test_catalog') }}
)

, affected_columns as (
    select
          test_name
        , input_table_name
        , input_column_name
    from {{ ref('data_quality__logical_test_input_columns') }}
)

, input_layer_columns as (
    select
          input_table_name
        , input_column_name
    from {{ ref('data_quality__input_layer_catalog') }}
)

, missing_mappings as (
    select
          catalog_tests.test_name
        , catalog_tests.input_table_name
        , cast(null as {{ dbt.type_string() }}) as input_column_name
        , {{ dq_string_literal_sql('missing_affected_column') }} as issue_type
    from catalog_tests
    left join affected_columns
        on catalog_tests.test_name = affected_columns.test_name
       and catalog_tests.input_table_name = affected_columns.input_table_name
    where affected_columns.test_name is null
)

, invalid_mappings as (
    select
          affected_columns.test_name
        , affected_columns.input_table_name
        , affected_columns.input_column_name
        , case
            when affected_columns.input_column_name = '__record__'
                then {{ dq_string_literal_sql('placeholder_affected_column') }}
            when catalog_tests.test_name is null
                then {{ dq_string_literal_sql('mapping_without_catalog_test') }}
            else {{ dq_string_literal_sql('affected_column_not_in_input_contract') }}
          end as issue_type
    from affected_columns
    left join catalog_tests
        on affected_columns.test_name = catalog_tests.test_name
       and affected_columns.input_table_name = catalog_tests.input_table_name
    left join input_layer_columns
        on affected_columns.input_table_name = input_layer_columns.input_table_name
       and affected_columns.input_column_name = input_layer_columns.input_column_name
    where affected_columns.input_column_name = '__record__'
       or catalog_tests.test_name is null
       or input_layer_columns.input_column_name is null
)

select * from missing_mappings
union all
select * from invalid_mappings
