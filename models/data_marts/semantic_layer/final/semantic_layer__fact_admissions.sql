{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    e.encounter_id
  , e.person_id
  , {{ concat_strings(["e.person_id", "'|'", the_tuva_project.yyyymm("e.encounter_start_date")]) }} as member_month_sk
  , r.index_admission_flag
  , r.had_readmission_flag
  , r.planned_flag
  , r.readmit_30_flag
  , r.unplanned_readmit_30_flag
  , e.encounter_start_date as admit_date
  , e.encounter_end_date as discharge_date
  , e.length_of_stay
  , e.admit_source_code
  , e.admit_source_description
  , e.admit_type_code
  , e.admit_type_description
  , e.discharge_disposition_code
  , e.discharge_disposition_description
  , e.attending_provider_id
  , e.attending_provider_name
  , e.drg_code_type
  , e.drg_code
  , e.drg_description
  , r.readmission_encounter_id
  , r.days_to_readmit
  , r.readmission_admit_date
  , r.readmission_discharge_date
  , r.readmission_discharge_disposition_code
  , r.readmission_facility
  , r.readmission_drg_code_type
  , r.readmission_drg
  , r.readmission_length_of_stay
  , r.readmission_index_admission_flag
  , r.readmission_planned_flag
  , r.readmission_specialty_cohort
  , r.readmission_died_flag
  , r.readmission_diagnosis_ccs
  , case when p.encounter_id is not null then 1 else 0 end as pqi_flag
  , p.pqi_number
  , p.pqi_name
  , e.paid_amount
  , e.allowed_amount
  , e.charge_amount
  , e.claim_count
  , e.inst_claim_count
  , e.prof_claim_count
  , e.tuva_last_run
FROM {{ ref('semantic_layer__stg_readmissions__encounter_augmented') }} as ea 
LEFT JOIN {{ ref('semantic_layer__stg_core__encounter') }} as e ON ea.encounter_id = e.encounter_id
LEFT JOIN {{ ref('semantic_layer__stg_readmissions__readmission_summary') }} as r ON r.encounter_id = ea.encounter_id
LEFT JOIN {{ ref('semantic_layer__stg_ahrq_measures__pqi_summary') }} as p ON p.encounter_id = ea.encounter_id
