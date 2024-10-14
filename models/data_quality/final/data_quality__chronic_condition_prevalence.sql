with condition_counts as (
    select
        condition
        , count(patient_id) as patients
    from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }}
    group by condition
)
, total_patients as (
    select 
        count(distinct patient_id) as total_distinct_patients
    from {{ ref('core__conditions') }}
)

select
    conditions.condition
    , conditions.patients
    , (conditions.patients / total_patients.total_distinct_patients) * 100 as percent_of_total
    , null as medicare_benchmark
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from condition_counts as conditions
cross join total_patients
order by conditions.condition