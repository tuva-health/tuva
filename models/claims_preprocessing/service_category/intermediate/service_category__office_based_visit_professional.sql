{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct 
    med.claim_id
    , med.claim_line_number
    , med.claim_line_id
, 'Office-Based Visit' as service_category_2
, 'Office-Based Visit' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} med
inner join  {{ ref('service_category__stg_office_based') }} prof on med.claim_id = prof.claim_id
and med.claim_line_number = prof.claim_line_number
  and place_of_service_code in ('11','02')
  and ccs_category = '227' --consultation eval and preventative care