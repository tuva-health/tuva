{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
  claim_id
, claim_line_number
, claim_line_id
, 'inpatient' as service_category_1
, 'inpatient rehabilitation' as service_category_2
, 'inpatient rehabilitation' as service_category_3
, '{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }}
where claim_type = 'professional'
  and place_of_service_code in ('61')
