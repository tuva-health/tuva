{{ config(
     enabled = var('quality_measures_reporting_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }}