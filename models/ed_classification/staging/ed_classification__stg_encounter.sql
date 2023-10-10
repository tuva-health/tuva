{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    encounter_id
    , encounter_type
    , patient_id
    , encounter_end_date
    , facility_npi
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , primary_diagnosis_description
    , paid_amount
    , allowed_amount
    , charge_amount
from {{ ref('core__encounter') }}