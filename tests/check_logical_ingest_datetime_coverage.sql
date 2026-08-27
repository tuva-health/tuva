{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical', 'dq_rollup']
   )
}}

with input_tables as (
    select distinct input_table_name
    from {{ ref('data_quality__input_layer_catalog') }}
)

, ingest_contracts as (
    select
          input_table_name
        , count(*) as contract_count
        , min(lower(expected_data_type)) as minimum_data_type
        , max(lower(expected_data_type)) as maximum_data_type
        , min(expected_type_family) as minimum_type_family
        , max(expected_type_family) as maximum_type_family
    from {{ ref('data_quality__input_layer_catalog') }}
    where input_column_name = 'ingest_datetime'
    group by input_table_name
)

, ingest_tests as (
    select
          test_columns.input_table_name
        , count(*) as test_count
        , count(distinct test_columns.test_name) as distinct_test_count
        , min(test_columns.test_name) as minimum_test_name
        , max(test_columns.test_name) as maximum_test_name
        , min(test_catalog.test_type) as minimum_test_type
        , max(test_catalog.test_type) as maximum_test_type
        , min(test_catalog.severity) as minimum_severity
        , max(test_catalog.severity) as maximum_severity
    from {{ ref('data_quality__logical_test_input_columns') }} as test_columns
    inner join {{ ref('data_quality__logical_test_catalog') }} as test_catalog
        on test_columns.test_name = test_catalog.test_name
       and test_columns.input_table_name = test_catalog.input_table_name
    where test_columns.input_column_name = 'ingest_datetime'
    group by test_columns.input_table_name
)

, coverage_violations as (
    select
          input_tables.input_table_name
        , coalesce(ingest_contracts.contract_count, 0) as contract_count
        , ingest_contracts.minimum_data_type
        , ingest_contracts.maximum_data_type
        , ingest_contracts.minimum_type_family
        , ingest_contracts.maximum_type_family
        , coalesce(ingest_tests.test_count, 0) as test_count
        , coalesce(ingest_tests.distinct_test_count, 0) as distinct_test_count
        , ingest_tests.minimum_test_name
        , ingest_tests.maximum_test_name
        , ingest_tests.minimum_test_type
        , ingest_tests.maximum_test_type
        , ingest_tests.minimum_severity
        , ingest_tests.maximum_severity
    from input_tables
    left join ingest_contracts
        on input_tables.input_table_name = ingest_contracts.input_table_name
    left join ingest_tests
        on input_tables.input_table_name = ingest_tests.input_table_name
    where coalesce(ingest_contracts.contract_count, 0) <> 1
       or ingest_contracts.minimum_data_type <> 'timestamp'
       or ingest_contracts.maximum_data_type <> 'timestamp'
       or ingest_contracts.minimum_type_family <> 'timestamp'
       or ingest_contracts.maximum_type_family <> 'timestamp'
       or coalesce(ingest_tests.test_count, 0) <> 1
       or coalesce(ingest_tests.distinct_test_count, 0) <> 1
       or ingest_tests.minimum_test_name <> {{ concat_custom([
            'input_tables.input_table_name',
            dq_string_literal_sql('__ingest_datetime_out_of_reasonable_range')
          ]) }}
       or ingest_tests.maximum_test_name <> {{ concat_custom([
            'input_tables.input_table_name',
            dq_string_literal_sql('__ingest_datetime_out_of_reasonable_range')
          ]) }}
       or ingest_tests.minimum_test_type <> 'invalid'
       or ingest_tests.maximum_test_type <> 'invalid'
       or ingest_tests.minimum_severity <> 2
       or ingest_tests.maximum_severity <> 2
)

select *
from coverage_violations

{% if (the_tuva_project.tuva_boolean_var('claims_enabled', false))
      and (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
      and (the_tuva_project.tuva_boolean_var('provider_attribution_enabled', false)) %}
union all
select
      '__enabled_input_inventory__' as input_table_name
    , count(*) as contract_count
    , cast(null as {{ dbt.type_string() }}) as minimum_data_type
    , cast(null as {{ dbt.type_string() }}) as maximum_data_type
    , cast(null as {{ dbt.type_string() }}) as minimum_type_family
    , cast(null as {{ dbt.type_string() }}) as maximum_type_family
    , cast(null as {{ dbt.type_int() }}) as test_count
    , cast(null as {{ dbt.type_int() }}) as distinct_test_count
    , cast(null as {{ dbt.type_string() }}) as minimum_test_name
    , cast(null as {{ dbt.type_string() }}) as maximum_test_name
    , cast(null as {{ dbt.type_string() }}) as minimum_test_type
    , cast(null as {{ dbt.type_string() }}) as maximum_test_type
    , cast(null as {{ dbt.type_int() }}) as minimum_severity
    , cast(null as {{ dbt.type_int() }}) as maximum_severity
from input_tables
having count(*) <> 15
{% endif %}
