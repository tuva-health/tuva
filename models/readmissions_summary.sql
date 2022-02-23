

{{ config(materialized='table') }}


select
    aa.encounter_id,
    aa.patient_id,
    aa.admit_date,
    aa.discharge_date,
    aa.discharge_status_code,
    aa.facility,
    aa.ms_drg,
    aa.index_admission_flag,
    aa.planned_flag,
    aa.specialty_cohort,
    aa.died_flag,
    aa.diagnosis_ccs,
    aa.disqualified_encounter,
    aa.missing_admit_date,
    aa.missing_discharge_date,
    aa.admit_after_discharge,
    aa.missing_discharge_status_code,
    aa.invalid_discharge_status_code,
    aa.missing_primary_diagnosis,
    aa.invalid_primary_diagnosis_code,
    aa.no_diagnosis_ccs,
    aa.multiple_primary_diagnoses,
    bb.days_to_readmit,
    bb.readmit_30_flag,
    bb.unplanned_readmit_30_flag
    
from {{ ref('encounters_augmented') }} aa
     left join {{ ref('readmission_calculation') }} bb
     on aa.encounter_id = bb.encounter_id
