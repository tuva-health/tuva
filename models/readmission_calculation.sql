
-- Here calculate days to readmission for encounters
-- that had a readmission and create readmission flags


{{ config(materialized='view') }}




-- We create the encounter_sequence integer count
-- which keeps track of what number of encounter each
-- encounter is for a given patient
with encounter_sequence as (
select
    patient_id,
    encounter_id,
    admit_date,
    discharge_date,
    case
        when encounter_id in (select * from {{ ref('planned_encounters') }} )
	    then 1
	else 0
    end as planned_flag,
    row_number() over(
        partition by patient_id order by admit_date, discharge_date
    ) as encounter_sequence
from {{ ref('encounters_augmented') }}
where disqualified_encounter = 0
),


readmission_calc as (
select
    aa.patient_id,
    aa.encounter_id,
    bb.planned_flag as planned_readmission_flag,
    bb.admit_date - aa.discharge_date as days_to_readmit
from
    encounter_sequence aa
    left join encounter_sequence bb
    on aa.patient_id = bb.patient_id
    and aa.encounter_sequence + 1 = bb.encounter_sequence
),


readmit_flags as (
select
    encounter_id,
    days_to_readmit,
    case
        when days_to_readmit <= 30 then 1
	else 0
    end as readmit_30_flag,
    case
        when (days_to_readmit <= 30) and (planned_readmission_flag = 0) then 1
	else 0
    end as unplanned_readmit_30_flag    
from readmission_calc    
)




select *
from readmit_flags
