{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

{% if (var('data_quality_enabled', false) | as_bool)
    and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)) %}
    select
          data_source
        , input_table_name
        , test_name
        , total_row_count
        , tested_count
        , failed_count
        , passed_count
        , not_applicable_count
    from {{ ref('data_quality__logical_test_results') }}
    where total_row_count <> tested_count + not_applicable_count
       or tested_count <> failed_count + passed_count
       or total_row_count < 0
       or tested_count < 0
       or failed_count < 0
       or passed_count < 0
       or not_applicable_count < 0
{% else %}
    select
          cast(null as {{ dbt.type_string() }}) as data_source
        , cast(null as {{ dbt.type_string() }}) as input_table_name
        , cast(null as {{ dbt.type_string() }}) as test_name
        , cast(null as {{ dbt.type_bigint() }}) as total_row_count
        , cast(null as {{ dbt.type_bigint() }}) as tested_count
        , cast(null as {{ dbt.type_bigint() }}) as failed_count
        , cast(null as {{ dbt.type_bigint() }}) as passed_count
        , cast(null as {{ dbt.type_bigint() }}) as not_applicable_count
    {{ dq_empty_result_guard_sql() }}
{% endif %}
