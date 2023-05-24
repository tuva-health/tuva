{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  a.claim_id
, a.claim_line_number
, 'Ambulatory Surgery' as service_category_2
from {{ ref('input_layer__medical_claim') }} a
left join {{ ref('dme_professional') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
where a.claim_type = 'professional'
  and a.place_of_service_code in ('24')
  and (b.claim_id is null and b.claim_line_number is null)