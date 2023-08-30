{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

select distinct 
  claim_id
, claim_line_number
, 'Ambulance' as service_category_2
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and (hcpcs_code between 'A0425' and 'A0436' or place_of_service_code in ('41','42'))
  