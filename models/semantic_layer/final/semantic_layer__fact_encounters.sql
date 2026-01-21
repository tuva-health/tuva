{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select
    encounter_id
  , person_id
  , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
  , {{ dbt.concat(["person_id", "'|'", year_month('encounter_start_date')]) }} as member_month_sk
  , {{ year_month('encounter_start_date') }} as year_month
  , eg.encounter_group_sk
  , et.encounter_type_sk
  , encounter_start_date
  , encounter_end_date
  , attending_provider_id
  , attending_provider_name
  , facility_id
  , facility_name
  , facility_type
  , observation_flag
  , lab_flag
  , dme_flag
  , ambulance_flag
  , pharmacy_flag
  , ed_flag
  , delivery_flag
  , delivery_type
  , newborn_flag
  , nicu_flag
  , primary_diagnosis_code_type
  , primary_diagnosis_code
  , primary_diagnosis_description
  , paid_amount
  , allowed_amount
  , charge_amount
  , claim_count
  , inst_claim_count
  , prof_claim_count
  , source_model
  , data_source
  , encounter_source_type
  , e.tuva_last_run
from {{ ref('semantic_layer__stg_core__encounter') }} as e
inner join {{ ref('semantic_layer__dim_encounter_group') }} as eg on e.encounter_group = eg.encounter_group
inner join {{ ref('semantic_layer__dim_encounter_type') }} as et on e.encounter_type = et.encounter_type