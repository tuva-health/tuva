select
    encounter_id
    , person_id
    , {{ dbt.concat(["person_id", "'|'", "data_source"]) }} as patient_source_key
    , TO_CHAR(encounter_start_date, 'YYYYMM') as year_month
    , eg.encounter_group_sk
    , et.encounter_type_sk
    , encounter_start_date
    , encounter_end_date
    , length_of_stay
    , admit_source_code
    , admit_source_description
    , admit_type_code
    , admit_type_description
    , discharge_disposition_code
    , discharge_disposition_description
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
    , drg_code_type
    , drg_code
    , drg_description
    , paid_amount
    , allowed_amount
    , charge_amount
    , claim_count
    , inst_claim_count
    , prof_claim_count
    , source_model
    , data_source
    , tuva_last_run
    , encounter_source_type
from {{ ref('core__encounter') }} e
inner join {{ ref('power_bi__dim_encounter_group') }} eg on e.encounter_group = eg.encounter_group
inner join {{ ref('power_bi__dim_encounter_type') }} et on e.encounter_type = et.encounter_type