{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with total_cte as (
select count(*) as total_encounters
from {{ ref('core__encounter') }}
where encounter_type = 'acute inpatient'
) 

,cte as (
select concat(ms_drg_code, ' - ', ms_drg_description) ms_drg_code_and_description
,'acute inpatient drg distribution' as analytics_concept
,count(*)/total_encounters as encounter_percent
,total_encounters
,row_number() over (order by count(*) desc) as rank_nbr
from {{ ref('core__encounter') }}
cross join total_cte 
where encounter_type = 'acute inpatient'
and
ms_drg_code is not null
group by concat(ms_drg_code, ' - ', ms_drg_description)
,total_encounters
)

select cte.analytics_concept
,ms_drg_code_and_description as analytics_measure
,encounter_percent as data_source_value
,m.analytics_value
,rank_nbr as value_rank
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join cte on
m.analytics_concept = cte.analytics_concept
and
cte.rank_nbr = m.value_rank
where m.analytics_concept = 'acute inpatient drg distribution'
and rank_nbr < 11



