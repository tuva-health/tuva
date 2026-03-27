{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

select
    mc.medical_claim_id
  , mc.encounter_id
  , mc.person_id
  , mc.claim_id
  , mc.claim_line_number
  , mc.claim_type
  , mc.payer
  , mc.{{ quote_column('plan') }}
  , mc.claim_start_date
  , mc.claim_end_date
  , mc.claim_line_start_date
  , mc.claim_line_end_date
  , mc.admission_date
  , mc.discharge_date
  , mc.admit_source_code
  , mc.admit_source_description
  , mc.admit_type_code
  , mc.admit_type_description
  , mc.discharge_disposition_code
  , mc.discharge_disposition_description
  , mc.place_of_service_code
  , mc.place_of_service_description
  , mc.bill_type_code
  , mc.bill_type_description
  , mc.drg_code_type
  , mc.drg_code
  , mc.drg_description
  , mc.revenue_center_code
  , mc.revenue_center_description
  , mc.service_unit_quantity
  , mc.hcpcs_code
  , mc.hcpcs_modifier_1
  , mc.hcpcs_modifier_2
  , mc.hcpcs_modifier_3
  , mc.hcpcs_modifier_4
  , mc.hcpcs_modifier_5
  , mc.rendering_id
  , mc.rendering_tin
  , mc.rendering_name
  , mc.billing_id
  , mc.billing_tin
  , mc.billing_name
  , mc.facility_id
  , mc.facility_name
  , mc.paid_date
  , mc.paid_amount
  , mc.allowed_amount
  , mc.charge_amount
  , mc.coinsurance_amount
  , mc.copayment_amount
  , mc.deductible_amount
  , mc.total_cost_amount
  , mc.in_network_flag
  , mc.data_source
  , mc.service_category_1
  , mc.service_category_2
  , mc.service_category_3
  , mc.enrollment_flag
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('core__medical_claim') }} as mc