{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

with condition_counts as (
    select
        condition
        , count(distinct patient_id) as patients
    from {{ ref('chronic_conditions__cms_chronic_conditions_long') }}
    group by condition
)
, total_patients as (
    select 
        count(distinct patient_id) as total_distinct_patients
    from {{ ref('core__eligibility') }}
)
, results as (
select
    conditions.condition
    , conditions.patients
    , (conditions.patients / total_patients.total_distinct_patients) * 100 as percent_of_total
    , ROW_NUMBER() OVER (ORDER BY (conditions.patients / total_distinct_patients) * 100 DESC) AS condition_rank
from condition_counts as conditions
cross join total_patients
)

select
    results.condition
    , results.patients
    , results.condition_rank
    , results.percent_of_total
    , ref_data.analytics_value as percent_of_medicare_lds_total
    , ref_data.rank as medicare_lds_condition_rank
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from results
full outer join {{ ref('data_quality__medicare_reference_data') }} as ref_data on results.condition = ref_data.analytics_measure --Still need to add seed file
where ref_data.analytics_concept = 'Chronic Condition Top 10'
order by
    results.condition_rank