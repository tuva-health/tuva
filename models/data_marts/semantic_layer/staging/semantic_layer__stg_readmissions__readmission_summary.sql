{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    r.encounter_id
  , r.index_admission_flag
  , r.had_readmission_flag
  , r.planned_flag
  , r.readmit_30_flag
  , r.unplanned_readmit_30_flag
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
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('readmissions__readmission_summary') }} as r