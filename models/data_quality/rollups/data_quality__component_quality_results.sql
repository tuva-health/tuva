{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false))
       and ((the_tuva_project.tuva_boolean_var('claims_enabled', false)) or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'component_quality_results',
     tags = ['data_quality', 'dq_rollup'],
     materialized = 'table'
   )
}}

select
      data_source
    , domain_group_key
    , domain_group_name
    , component_key
    , component_name
    , cast(sum(cast(coalesce(total_row_count, 0) as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as total_row_count
    , cast(sum(cast(coalesce(tested_count, 0) as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as tested_count
    , cast(sum(cast(coalesce(passed_count, 0) as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as passed_count
    , cast(sum(cast(coalesce(failed_count, 0) as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as failed_count
    , cast(sum(cast(case when severity = 1 then coalesce(failed_count, 0) else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as severity_1_failed_count
    , cast(sum(cast(case when severity = 2 then coalesce(failed_count, 0) else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as severity_2_failed_count
    , cast(sum(cast(case when severity = 3 then coalesce(failed_count, 0) else 0 end as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as severity_3_failed_count
    , cast(sum(cast(coalesce(not_applicable_count, 0) as {{ dbt.type_bigint() }})) as {{ dbt.type_bigint() }}) as not_applicable_count
    , {{ dq_safe_ratio_sql('sum(coalesce(passed_count, 0))', 'sum(coalesce(tested_count, 0))') }} as pct_passed
    , {{ dq_safe_ratio_sql('sum(case when severity = 1 then coalesce(failed_count, 0) else 0 end)', 'sum(coalesce(tested_count, 0))') }} as pct_severity_1
    , {{ dq_safe_ratio_sql('sum(case when severity = 2 then coalesce(failed_count, 0) else 0 end)', 'sum(coalesce(tested_count, 0))') }} as pct_severity_2
    , {{ dq_safe_ratio_sql('sum(case when severity = 3 then coalesce(failed_count, 0) else 0 end)', 'sum(coalesce(tested_count, 0))') }} as pct_severity_3
    , case
        when sum(coalesce(tested_count, 0)) = 0 then 'not_applicable'
        when sum(case when severity = 1 then coalesce(failed_count, 0) else 0 end) > 0 then 'blocked'
        when sum(coalesce(failed_count, 0)) > 0 then 'issues'
        else 'passing'
      end as status
from {{ ref('data_quality__component_test_quality_results') }}
group by
      data_source
    , domain_group_key
    , domain_group_name
    , component_key
    , component_name
