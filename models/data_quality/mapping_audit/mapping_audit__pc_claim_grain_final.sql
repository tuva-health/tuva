{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}




select
  claim_line_number.claim_id,
  claim_line_number.claim_line_number_unique,

  member_id.member_id_always_populated,
  member_id.member_id_unique,

  plan.plan_always_populated,
  plan.plan_unique,

  ndc_code.ndc_code_correct_length,
  ndc_code.ndc_code_always_populated,

  quantity.quantity_is_positive_integer,

  days_supply.days_supply_is_positive_integer,

  refills.refills_is_positive_integer,

  in_network_flag.in_network_flag_unique,
  in_network_flag.in_network_flag_valid_values,

  data_source.data_source_always_populated,
  data_source.data_source_unique
  
from {{ ref('mapping_audit__pc_claim_line_number') }} claim_line_number

left join {{ ref('mapping_audit__pc_member_id') }} member_id
on claim_line_number.claim_id = member_id.claim_id

left join {{ ref('mapping_audit__pc_plan') }} plan
on claim_line_number.claim_id = plan.claim_id

left join {{ ref('mapping_audit__pc_ndc_code') }} ndc_code
on claim_line_number.claim_id = ndc_code.claim_id

left join {{ ref('mapping_audit__pc_quantity') }} quantity
on claim_line_number.claim_id = quantity.claim_id

left join {{ ref('mapping_audit__pc_days_supply') }} days_supply
on claim_line_number.claim_id = days_supply.claim_id

left join {{ ref('mapping_audit__pc_refills') }} refills
on claim_line_number.claim_id = refills.claim_id

left join {{ ref('mapping_audit__pc_in_network_flag') }} in_network_flag
on claim_line_number.claim_id = in_network_flag.claim_id

left join {{ ref('mapping_audit__pc_data_source') }} data_source
on claim_line_number.claim_id = data_source.claim_id
