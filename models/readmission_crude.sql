
-- Here we calculate readmissions using all encounters
-- that have valid admit and discharge dates and no overlap.
-- This is meant to give a crude sense of the readmission
-- rate without taking into account all the CMS HWR logic.


{{ config(materialize='table')  }}



with encounter_info as (
select
    encounter_id,
    patient_id,
    admit_date,
    discharge_date
from {{ ref('stg_encounter') }}
where
    admit_date is not null
    and
    discharge_date is not null
    and
    admit_date <= discharge_date
    and
    encounter_id not in (select distinct encounter_id_A
	                         from {{ ref('encounter_overlap') }} )
    and
    encounter_id not in (select distinct encounter_id_B
	                         from {{ ref('encounter_overlap') }} )
),


encounter_sequence as (
select
    encounter_id,
    patient_id,
    admit_date,
    discharge_date,
    row_number() over(
        partition by patient_id order by admit_date, discharge_date
    ) as encounter_sequence    
from encounter_info
),


readmission_calc as (
select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    case
        when bb.encounter_id is not null then 1
	else 0
    end as had_readmission_flag,
    bb.admit_date - aa.discharge_date as days_to_readmit,
    case
        when (bb.admit_date - aa.discharge_date) <= 30  then 1
	else 0
    end as readmit_30_flag
from encounter_sequence aa left join encounter_sequence bb
     on aa.patient_id = bb.patient_id
     and aa.encounter_sequence + 1 = bb.encounter_sequence
)



select *
from readmission_calc

