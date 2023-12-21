{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
      patient_id
    , patient_id||data_source||payer||plan||enrollment_start_date||enrollment_end_date as patient_id_key
    , member_id
    , gender
    , race
    , birth_date
    , death_date
    , death_flag
    , enrollment_start_date
    , enrollment_end_date
    , payer
    , payer_type
    , plan
    , original_reason_entitlement_code
    , dual_status_code
    , medicare_status_code
    , first_name
    , last_name
    , address
    , city
    , state
    , zip_code
    , phone
    , data_source
from {{ ref('eligibility') }}