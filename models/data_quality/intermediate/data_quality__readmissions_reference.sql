{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with cte as (
select 
'readmission rate' as analytics_measure
,'readmissions' as analytics_concept
,sum(unplanned_readmit_30_flag)/cast(count(*) as decimal(18,2)) as data_source_value
from {{ ref('readmissions__readmission_summary') }}  r 
where index_admission_flag = 1
)

select 
 m.analytics_concept as analytics_concept
,m.analytics_measure as analytics_measure
,cte.data_source_value
,m.analytics_value
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join cte on cte.analytics_measure = m.analytics_measure
and
m.analytics_concept = cte.analytics_concept
where m.analytics_concept = 'readmissions'




