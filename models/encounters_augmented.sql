
-- Here we list all encounters from the input stg_encounter
-- table and we augment them with extra fields
-- that are relevant for readmission measures


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
        when bb.encounter_id is not null then 1
	else 0
    end as index_admission_flag,
    case
        when cc.encounter_id is not null then 1
	else 0
    end as planned_flag,
    dd.specialty_cohort,
    case
        when aa.discharge_status_code = '20' then 1
	else 0
    end as died_flag,
    ee.diagnosis_ccs,
    ee.disqualified_encounter,
    ee.missing_admit_date,
    ee.missing_discharge_date,
    ee.admit_after_discharge,
    ee.missing_discharge_status_code,
    ee.invalid_discharge_status_code,
    ee.missing_primary_diagnosis,
    ee.multiple_primary_diagnoses,
    ee.invalid_primary_diagnosis_code,
    ee.no_diagnosis_ccs

    
from
    {{ var('src_encounter') }} aa
    left join {{ ref('index_admissions') }} bb
    on aa.encounter_id = bb.encounter_id
    left join {{ ref('planned_encounters') }} cc
    on aa.encounter_id = cc.encounter_id 
    left join {{ ref('encounter_specialty_cohorts') }} dd
    on aa.encounter_id = dd.encounter_id
    left join {{ ref('disqualified_encounters') }} ee
    on aa.encounter_id = ee.encounter_id
)



select *
from encounters_augmented
