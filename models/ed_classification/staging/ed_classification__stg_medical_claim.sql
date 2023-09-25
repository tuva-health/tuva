{{ config(
     enabled = var('ed_classification_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select
    claim_id
    , claim_line_number
    , encounter_id
    , patient_id
    , claim_line_end_date
    , service_category_1
    , service_category_2
    , place_of_service_code
    , revenue_center_code
    , billing_npi
    , facility_npi
    , paid_amount
from {{ ref('core__medical_claim') }}