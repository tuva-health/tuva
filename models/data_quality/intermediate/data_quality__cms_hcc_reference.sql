{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select avg(normalized_risk_score) data_source_value
,cast('normalized risk score' as {{ dbt.type_string() }}) as analytics_measure
,cast('cms-hcc' as {{ dbt.type_string() }}) as analytics_concept
from {{ ref('cms_hcc__patient_risk_scores') }}
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
where m.analytics_concept = 'cms-hcc'
and
m.analytics_measure = 'normalized risk score'
