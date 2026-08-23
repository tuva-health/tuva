{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool)
       and ((var('claims_enabled', false) | as_bool) or (var('clinical_enabled', false) | as_bool)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'component_quality_results',
     tags = ['data_quality', 'dq', 'dq1', 'dq_rollup'],
     materialized = 'table'
   )
}}

select
      data_source
    , domain_group_key
    , domain_group_name
    , component_key
    , component_name
    , cast(sum(coalesce(total_row_count, 0)) as {{ dbt.type_int() }}) as total_row_count
    , cast(sum(coalesce(tested_count, 0)) as {{ dbt.type_int() }}) as tested_count
    , cast(sum(coalesce(passed_count, 0)) as {{ dbt.type_int() }}) as passed_count
    , cast(sum(coalesce(failed_count, 0)) as {{ dbt.type_int() }}) as failed_count
    , cast(sum(case when severity = 1 then coalesce(failed_count, 0) else 0 end) as {{ dbt.type_int() }}) as severity_1_failed_count
    , cast(sum(case when severity = 2 then coalesce(failed_count, 0) else 0 end) as {{ dbt.type_int() }}) as severity_2_failed_count
    , cast(sum(case when severity = 3 then coalesce(failed_count, 0) else 0 end) as {{ dbt.type_int() }}) as severity_3_failed_count
    , cast(sum(coalesce(not_applicable_count, 0)) as {{ dbt.type_int() }}) as not_applicable_count
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
