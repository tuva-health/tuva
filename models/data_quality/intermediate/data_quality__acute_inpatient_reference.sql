{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select avg(length_of_stay) los
,sum(case when discharge_disposition_code in ('20','40','41','42') then 1 else 0 end) deceased_encounters
,sum(case when discharge_disposition_code in ('20','40','41','42') then 1 else 0 end)/count(*) as mortality_rate
from {{ ref('core__encounter') }}
where encounter_type = 'acute inpatient'
)


,long_cte as (
   select 'acute inpatient' as analytics_concept
,'length of stay' as analytics_measure
,los as data_source_value
from cte

union all 

   select 'acute inpatient' as analytics_concept
,'mortality rate' as analytics_measure
,mortality_rate as data_source_value
from cte
)


select m.analytics_concept
,m.analytics_measure
,data_source_value
,m.analytics_value
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join long_cte on long_cte.analytics_measure = m.analytics_measure
and
m.analytics_concept = long_cte.analytics_concept
where m.analytics_concept = 'acute inpatient'
and
m.analytics_measure = 'length of stay'

union all 

select m.analytics_concept
,m.analytics_measure
,data_source_value
,m.analytics_value
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__reference_mart_analytics') }} m 
left join long_cte on long_cte.analytics_measure = m.analytics_measure
and
m.analytics_concept = long_cte.analytics_concept
where m.analytics_concept = 'acute inpatient'
and
m.analytics_measure = 'mortality rate'






