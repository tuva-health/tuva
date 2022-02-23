
-- Here we list all encounters from the input stg_encounter
-- table and we augment them with a few extra fields
-- that give information relevant for index admissions


{{ config(materialized='table') }}



with encounters_augmented as (
select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    aa.discharge_status_code,
    aa.facility,
    aa.ms_drg,
    case
        when
	    aa.encounter_id in (select *
	                        from {{ ref('index_admissions') }} )
	then 1
	else 0
    end as index_admission_flag,
    case
        when
	    aa.encounter_id in (select *
	                        from {{ ref('planned_encounters') }} )
	then 1
	else 0
    end as planned_flag,
    bb.specialty_cohort,
    case
        when aa.encounter_id in (select * from {{ ref('died') }} ) then 1
	else 0
    end as died_flag,
    cc.diagnosis_ccs,
    cc.disqualified_encounter,
    cc.missing_admit_date,
    cc.missing_discharge_date,
    cc.admit_after_discharge,
    cc.missing_discharge_status_code,
    cc.invalid_discharge_status_code,
    cc.missing_primary_diagnosis,
    cc.invalid_primary_diagnosis_code,
    cc.no_diagnosis_ccs,
    cc.multiple_primary_diagnoses
    
from {{ var('src_encounter') }} aa
     left join {{ ref('encounter_specialty_cohorts') }} bb
     on aa.encounter_id = bb.encounter_id
     left join {{ ref('disqualified_encounters') }} cc
     on aa.encounter_id = cc.encounter_id
)



select *
from encounters_augmented



