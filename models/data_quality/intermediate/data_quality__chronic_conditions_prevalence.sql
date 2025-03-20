{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with condition_counts as (
    select
        condition
        , count(distinct person_id) as patients
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
    group by condition
)
, total_patients as (
    select 
        count(distinct person_id) as total_distinct_patients
    from {{ ref('core__eligibility') }}
)

, results_first as (
select
    conditions.condition
    , conditions.patients
    , CASE 
        WHEN total_patients.total_distinct_patients = 0 THEN NULL
        ELSE (conditions.patients / total_patients.total_distinct_patients) * 100 
      END as percent_of_total

from condition_counts as conditions
cross join total_patients
)

,results_second as (
    select 
      condition
    , patients
    , percent_of_total
    , ROW_NUMBER() OVER (ORDER BY percent_of_total desc) AS condition_rank
    from results_first
)


select
    ref_data.analytics_concept
    , coalesce(results.condition, ref_data.analytics_measure) as analytics_measure
    , coalesce(results.patients,0) as patients
    , coalesce(results.percent_of_total,0) as data_source_value
    , ref_data.analytics_value
    , coalesce(results.condition_rank,0) as value_rank
    , ref_data.value_rank as medicare_lds_condition_rank
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('data_quality__reference_mart_analytics') }}  ref_data
left join results_second as results on results.condition = ref_data.analytics_measure
where ref_data.analytics_concept = 'chronic conditions top 10'
