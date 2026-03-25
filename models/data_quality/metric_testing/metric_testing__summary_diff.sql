{{ config(
     enabled = var('metric_testing_enabled', False) | as_bool
   )
}}

{% set baseline_relation = load_relation(ref('data_quality__metric_testing_summary_baseline')) %}

with baseline_metrics as (
    {% if baseline_relation is not none %}
    select
        metric_id
      , metric_value
    from {{ ref('data_quality__metric_testing_summary_baseline') }}
    {% else %}
    select
        cast(null as {{ dbt.type_string() }}) as metric_id
      , cast(null as {{ dbt.type_numeric() }}) as metric_value
    where 1 = 0
    {% endif %}
)

select
    current_metrics.metric_id
  , current_metrics.metric_name
  , current_metrics.metric_value
  , baseline_metrics.metric_value as baseline_metric_value
  , case
        when baseline_metrics.metric_value is null then null
        else cast(
            current_metrics.metric_value - baseline_metrics.metric_value
            as {{ dbt.type_numeric() }}
        )
    end as metric_diff
from {{ ref('metric_testing__summary') }} as current_metrics
left join baseline_metrics
    on current_metrics.metric_id = baseline_metrics.metric_id
