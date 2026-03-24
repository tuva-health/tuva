{{ config(
     enabled = (
       var('metric_testing_enabled', False) | as_bool
       and var('synthetic_data_size', 'small') == 'large'
     )
   )
}}

select
    metric_id
  , baseline_metric_value
  , metric_value
  , metric_diff
from {{ ref('metric_testing__summary_diff') }}
where baseline_metric_value is null
   or metric_diff != 0
