{{ config(
     enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
   )
}}

select
    nullif(claim_id, '') as claim_id
    , claim_line_number
    , nullif(patient_id, '') as patient_id
    , nullif(member_id, '') as member_id
    , nullif(prescribing_provider_npi, '') as prescribing_provider_npi
    , nullif(dispensing_provider_npi, '') as dispensing_provider_npi
    , dispensing_date
    , nullif(ndc_code, '') as ndc_code
    , quantity
    , days_supply
    , refills
    , paid_date
    , paid_amount
    , allowed_amount 
    , nullif(data_source, '') as data_source
from {{ var('pharmacy_claim') }}