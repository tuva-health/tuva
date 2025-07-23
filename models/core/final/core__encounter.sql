select
    encounter_sk
    , data_source
    , patient_sk
    , member_id
    , encounter_type
    , encounter_group
    , encounter_start_date
    , encounter_end_date
    , admit_age
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
    , {{ current_timestamp() }} as tuva_last_run
from {{ ref('the_tuva_project', 'core__stg_claims_encounter') }}