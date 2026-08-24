{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     severity = 'error',
     tags = ['data_quality', 'dq_logical', 'dq_rollup']
   )
}}

with native_date_columns as (
    select
          input_table_name
        , input_column_name
    from {{ ref('data_quality__input_layer_catalog') }}
    where expected_type_family = 'date'
)

, supported_date_tests as (
    select
          test_columns.input_table_name
        , test_columns.input_column_name
        , count(*) as test_count
        , max(test_catalog.test_type) as test_type
        , max(test_catalog.severity) as severity
    from {{ ref('data_quality__logical_test_input_columns') }} as test_columns
    inner join {{ ref('data_quality__logical_test_catalog') }} as test_catalog
        on test_columns.test_name = test_catalog.test_name
       and test_columns.input_table_name = test_catalog.input_table_name
    where test_catalog.flag_column_name = {{ concat_custom([
        'test_columns.input_column_name',
        dq_string_literal_sql('_outside_supported_date_range')
    ]) }}
    group by
          test_columns.input_table_name
        , test_columns.input_column_name
)

select
      native_date_columns.input_table_name
    , native_date_columns.input_column_name
    , coalesce(supported_date_tests.test_count, 0) as test_count
    , supported_date_tests.test_type
    , supported_date_tests.severity
from native_date_columns
left join supported_date_tests
    on native_date_columns.input_table_name = supported_date_tests.input_table_name
   and native_date_columns.input_column_name = supported_date_tests.input_column_name
where coalesce(supported_date_tests.test_count, 0) <> 1
   or supported_date_tests.test_type <> 'invalid'
   or supported_date_tests.severity <> 2
