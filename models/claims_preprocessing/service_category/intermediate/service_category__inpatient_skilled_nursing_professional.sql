{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct
    a.claim_id
    , a.claim_line_number
    , a.claim_line_id
    , 'Inpatient' as service_category_1
, 'Skilled Nursing' as service_category_2
, 'Skilled Nursing' as service_category_3
,'{{ this.name }}' as source_model_name
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('service_category__stg_medical_claim') }} a
left join {{ ref('service_category__dme_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where claim_type = 'professional'
  and place_of_service_code in ('31','32')
  and (b.claim_id is null and b.claim_line_number is null)
  