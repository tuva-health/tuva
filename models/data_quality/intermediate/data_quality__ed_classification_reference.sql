{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with total_cte as 
(
select count(*) as total_encounters
from {{ ref('core__encounter') }}
where encounter_type = 'emergency department'
) 

,final as (
select coalesce(ed_classification_description,'Not Classified') as ed_classification_description
,count(*) as encounters
,total_cte.total_encounters
,count(*)/total_cte.total_encounters as percent_of_total
,row_number() over (order by count(*) desc) as value_rank
from {{ ref('core__encounter') }} e
cross join total_cte
left join {{ ref('ed_classification__summary') }} s on e.encounter_id = s.encounter_id 
where encounter_type = 'emergency department'
group by coalesce(ed_classification_description,'Not Classified')
,total_cte.total_encounters
)

,actually_final as (
select 'ed classification' as analytics_concept
,ed_classification_description as analytics_measure
,percent_of_total as source_value
,value_rank as value_rank
from final
)


select m.analytics_concept
,m.analytics_measure
,source_value
,m.analytics_value
,m.value_rank
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join actually_final on actually_final.analytics_measure = m.analytics_measure
and
m.analytics_concept = actually_final.analytics_concept
where m.analytics_concept = 'ed classification'

