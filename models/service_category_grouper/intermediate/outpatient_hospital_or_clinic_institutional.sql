{{ config(
     enabled = var('service_category_grouper_enabled',var('tuva_marts_enabled',True))
   )
}}

select distinct 
  a.claim_id
, a.claim_line_number
from {{ ref('input_layer__medical_claim') }} a
left join {{ ref('emergency_department_institutional') }} b
  on a.claim_id = b.claim_id
  and b.claim_line_number = b.claim_line_number
left join {{ ref('urgent_care_institutional') }} c
  on a.claim_id = c.claim_id
  and a.claim_line_number = c.claim_line_number
where a.claim_type = 'institutional'
  and left(a.bill_type_code,2) in ('13','71','73')
  and (b.claim_id is null and b.claim_line_number is null)
  and (c.claim_id is null and c.claim_line_number is null)
  