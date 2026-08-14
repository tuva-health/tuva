{{ config(
     enabled = var('enable_data_quality', false) | as_bool
   )
}}

select
      data_source
    , domain_group_key
    , domain_group_name
    , cast(sum(coalesce(total_row_count, 0)) as {{ dbt.type_int() }}) as total_row_count
    , cast(sum(coalesce(tested_count, 0)) as {{ dbt.type_int() }}) as tested_count
    , cast(sum(coalesce(passed_count, 0)) as {{ dbt.type_int() }}) as passed_count
    , cast(sum(coalesce(failed_count, 0)) as {{ dbt.type_int() }}) as failed_count
    , cast(sum(coalesce(severity_1_failed_count, 0)) as {{ dbt.type_int() }}) as severity_1_failed_count
    , cast(sum(coalesce(severity_2_failed_count, 0)) as {{ dbt.type_int() }}) as severity_2_failed_count
    , cast(sum(coalesce(severity_3_failed_count, 0)) as {{ dbt.type_int() }}) as severity_3_failed_count
    , cast(sum(coalesce(not_applicable_count, 0)) as {{ dbt.type_int() }}) as not_applicable_count
    , {{ dq_safe_ratio_sql('sum(coalesce(passed_count, 0))', 'sum(coalesce(tested_count, 0))') }} as pct_passed
    , {{ dq_safe_ratio_sql('sum(coalesce(severity_1_failed_count, 0))', 'sum(coalesce(tested_count, 0))') }} as pct_severity_1
    , {{ dq_safe_ratio_sql('sum(coalesce(severity_2_failed_count, 0))', 'sum(coalesce(tested_count, 0))') }} as pct_severity_2
    , {{ dq_safe_ratio_sql('sum(coalesce(severity_3_failed_count, 0))', 'sum(coalesce(tested_count, 0))') }} as pct_severity_3
    , case
        when sum(coalesce(tested_count, 0)) = 0 then 'not_applicable'
        when sum(coalesce(severity_1_failed_count, 0)) > 0 then 'blocked'
        when sum(coalesce(failed_count, 0)) > 0 then 'issues'
        else 'passing'
      end as status
from {{ ref('data_quality__summary_by_component') }}
group by
      data_source
    , domain_group_key
    , domain_group_name
