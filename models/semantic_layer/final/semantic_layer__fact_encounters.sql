{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select
    encounter_id
  , person_id
  , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
  , {{ dbt.concat(["person_id", "'|'", "TO_CHAR(encounter_start_date, 'YYYYMM')"]) }} as member_month_sk
  , TO_CHAR(encounter_start_date, 'YYYYMM') as year_month
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
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__encounter') }} e
inner join {{ ref('semantic_layer__dim_encounter_group') }} eg on e.encounter_group = eg.encounter_group
inner join {{ ref('semantic_layer__dim_encounter_type') }} et on e.encounter_type = et.encounter_type