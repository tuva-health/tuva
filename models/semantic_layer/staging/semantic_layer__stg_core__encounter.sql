{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


SELECT
    e.encounter_id
  , e.person_id
  , e.encounter_type
  , e.encounter_group
  , e.encounter_start_date
  , e.encounter_end_date
  , e.length_of_stay
  , e.admit_source_code
  , e.admit_source_description
  , e.admit_type_code
  , e.admit_type_description
  , e.discharge_disposition_code
  , e.discharge_disposition_description
  , e.attending_provider_id
  , e.attending_provider_name
  , e.facility_id
  , e.facility_name
  , e.facility_type
  , e.observation_flag
  , e.lab_flag
  , e.dme_flag
  , e.ambulance_flag
  , e.pharmacy_flag
  , e.ed_flag
  , e.delivery_flag
  , e.delivery_type
  , e.newborn_flag
  , e.nicu_flag
  , e.snf_part_b_flag
  , e.primary_diagnosis_code_type
  , e.primary_diagnosis_code
  , e.primary_diagnosis_description
  , e.drg_code_type
  , e.drg_code
  , e.drg_description
  , e.paid_amount
  , e.allowed_amount
  , e.charge_amount
  , e.claim_count
  , e.inst_claim_count
  , e.prof_claim_count
  , e.source_model
  , e.data_source
  , e.encounter_source_type
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('core__encounter') }} as e