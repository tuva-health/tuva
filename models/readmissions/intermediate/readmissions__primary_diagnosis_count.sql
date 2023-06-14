{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

-- Every encounter should have one and only one primary
-- diagnosis. A potential data quality problem would
-- be to have encounters that have no primary diagnosis
-- or multiple primary diagnoses.
-- Here we list the count of primary diagnoses associated
-- with each encounter in the stg_diagnosis model.



-- Here we list the primary diagnosis count for every
-- encounter_id in the stg_diagnosis model that has
-- at least one primary diagnosis
with primary_diagnosis_count_greater_than_zero as (
select
    encounter_id,
    count(*) as primary_dx_count
from {{ ref('readmissions__diagnosis') }}
where diagnosis_rank = 1
group by encounter_id
),


-- Here we list all distinct encounter_ids in the
-- stg_diagnosis model
all_encounter_ids as (
select distinct encounter_id
from {{ ref('readmissions__diagnosis') }}
),


-- Here we list the primary diagnosis count for every
-- encounter_id in the stg_diagnosis model.
-- The primary_dx_count can be any
-- nonnegative integer: {0,1,2,3,...}
all_primary_diagnosis_count as (
select
    aa.encounter_id,
    case
        when bb.primary_dx_count is null then 0
	else bb.primary_dx_count
    end as primary_dx_count
from
    all_encounter_ids aa
    left join primary_diagnosis_count_greater_than_zero bb
    on aa.encounter_id = bb.encounter_id
)



select *, '{{ var('last_update')}}' as last_update
from all_primary_diagnosis_count
