{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with total_cte as 
(
select count(*) as total_encounters
from {{ ref('core__encounter') }}
where encounter_type = cast('emergency department' as {{dbt.type_string()}})
) 

,final as (
select coalesce(cast(ed_classification_description as {{dbt.type_string()}}), cast('Not Classified' as {{dbt.type_string()}})) as ed_classification_description
,count(*) as encounters
,total_cte.total_encounters
,count(*)/total_cte.total_encounters as percent_of_total
,row_number() over (order by count(*) desc) as value_rank
from {{ ref('core__encounter') }} e
cross join total_cte
left join {{ ref('ed_classification__summary') }} s on e.encounter_id = s.encounter_id 
where encounter_type = cast('emergency department' as {{dbt.type_string()}})
group by coalesce(cast(ed_classification_description as {{dbt.type_string()}}), cast('Not Classified' as {{dbt.type_string()}}))
,total_cte.total_encounters
)

,actually_final as (
select cast('ed classification' as {{dbt.type_string()}}) as analytics_concept
,ed_classification_description as analytics_measure
,percent_of_total as data_source_value
,value_rank as value_rank
from final
)

select m.analytics_concept
,m.analytics_measure
,data_source_value
,m.analytics_value
,m.value_rank
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join actually_final on actually_final.analytics_measure = m.analytics_measure
and
m.analytics_concept = actually_final.analytics_concept
where m.analytics_concept = cast('ed classification' as {{dbt.type_string()}})
