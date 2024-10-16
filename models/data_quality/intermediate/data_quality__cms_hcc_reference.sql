{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with cte as (
select avg(normalized_risk_score) data_source_value
,cast('cms-hcc score' as {{ dbt.type_string() }}) as analytics_measure
from {{ ref('cms_hcc__patient_risk_scores') }}
)

select cte.analytics_measure
,cte.data_source_value
,m.analytics_value
from cte
left join {{ ref('data_quality__reference_mart_analytics') }} m on cte.analytics_measure = m.analytics_measure

