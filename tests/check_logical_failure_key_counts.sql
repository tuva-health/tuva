{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and (the_tuva_project.tuva_boolean_var('enable_data_quality_failure_keys', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     severity = 'error',
     tags = ['data_quality', 'dq_logical']
   )
}}

with failure_key_counts as (
    select
          data_source
        , input_table_name
        , test_name
        , cast(
            sum(cast(1 as {{ dbt.type_bigint() }}))
            as {{ dbt.type_bigint() }}
          ) as failure_key_count
    from {{ ref('data_quality__logical_failure_keys') }}
    group by
          data_source
        , input_table_name
        , test_name
)

, logical_results as (
    select
          data_source
        , input_table_name
        , test_name
        , failed_count
    from {{ ref('data_quality__logical_test_results') }}
)

select
      coalesce(logical_results.data_source, failure_key_counts.data_source) as data_source
    , coalesce(logical_results.input_table_name, failure_key_counts.input_table_name) as input_table_name
    , coalesce(logical_results.test_name, failure_key_counts.test_name) as test_name
    , coalesce(logical_results.failed_count, 0) as failed_count
    , coalesce(failure_key_counts.failure_key_count, 0) as failure_key_count
from logical_results
full outer join failure_key_counts
    on (
        logical_results.data_source = failure_key_counts.data_source
        or (logical_results.data_source is null and failure_key_counts.data_source is null)
    )
   and logical_results.input_table_name = failure_key_counts.input_table_name
   and logical_results.test_name = failure_key_counts.test_name
where coalesce(logical_results.failed_count, 0)
      <> coalesce(failure_key_counts.failure_key_count, 0)
