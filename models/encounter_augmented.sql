
-- Here we list all encounters from the stg_encounter model
-- and we augment them with extra fields
-- that are relevant for readmission measures


{{ config(materialized='table') }}



with encounter_augmented as (
select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    aa.discharge_status_code,
    aa.facility,
    aa.ms_drg,
    aa.discharge_date - aa.admit_date  as length_of_stay,
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
    ee.disqualified_encounter_flag,
    ee.missing_admit_date_flag,
    ee.missing_discharge_date_flag,
    ee.admit_after_discharge_flag,
    ee.missing_discharge_status_code_flag,
    ee.invalid_discharge_status_code_flag,
    ee.missing_primary_diagnosis_flag,
    ee.multiple_primary_diagnoses_flag,
    ee.invalid_primary_diagnosis_code_flag,
    ee.no_diagnosis_ccs_flag,
    ee.overlaps_with_another_encounter_flag
    
from
    {{ ref('stg_encounter') }} aa
    left join {{ ref('index_admission') }} bb
    on aa.encounter_id = bb.encounter_id
    left join {{ ref('planned_encounter') }} cc
    on aa.encounter_id = cc.encounter_id 
    left join {{ ref('encounter_specialty_cohort') }} dd
    on aa.encounter_id = dd.encounter_id
    left join {{ ref('encounter_data_quality') }} ee
    on aa.encounter_id = ee.encounter_id
)



select *
from encounter_augmented
