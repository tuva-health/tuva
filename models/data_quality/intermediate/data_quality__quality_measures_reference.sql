{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select 
    cast('quality measures' as {{dbt.type_string()}}) as analytics_concept
    ,measure_name as analytics_measure
    ,performance_rate as data_source_value
from {{ ref('quality_measures__summary_counts') }}  r 
)

select 
    m.analytics_concept as analytics_concept
    ,m.analytics_measure as analytics_measure
    ,cte.data_source_value
    ,m.analytics_value
      , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join cte on cte.analytics_measure = m.analytics_measure
and
m.analytics_concept = cte.analytics_concept
where m.analytics_concept = cast('quality measures' as {{dbt.type_string()}})
