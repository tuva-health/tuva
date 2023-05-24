{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  a.claim_id
, a.claim_line_number
, 'Skilled Nursing' as service_category_2
from {{ ref('input_layer__medical_claim') }} a
left join {{ ref('service_category__dme_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where claim_type = 'professional'
  and place_of_service_code in ('31','32')
  and (b.claim_id is null and b.claim_line_number is null)
  