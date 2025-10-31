{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with encounter_sk as (
    select
        e.encounter_id
      , e.encounter_group_sk
      , e.encounter_type_sk
      , e.primary_diagnosis_code
      , e.primary_diagnosis_description
      , primary_provider_id
      , specialty
      , ccsr.ccsr_parent_category
      , ccsr.ccsr_category
      , ccsr.ccsr_category_description
    from {{ ref('semantic_layer__fact_encounters') }} e
    left join {{ ref('semantic_layer__dim_encounter_provider') }} p on e.encounter_id = p.encounter_id
    left join {{ ref('ccsr__dx_vertical_pivot') }} ccsr on e.primary_diagnosis_code = ccsr.code
        and ccsr.ccsr_category_rank = 1
)

select
    mc.medical_claim_id
  , mc.encounter_id
  , esk.encounter_group_sk
  , esk.encounter_type_sk
  , esk.primary_diagnosis_code
  , esk.primary_diagnosis_description
  , esk.primary_provider_id
  , esk.specialty
  , esk.ccsr_parent_category
  , esk.ccsr_category
  , esk.ccsr_category_description
  , mc.person_id
  , {{ dbt.concat(["mc.person_id", "'|'", "mc.data_source"]) }} as patient_source_key
  , {{ dbt.concat(["mc.person_id", "'|'", "TO_CHAR(claim_start_date, 'YYYYMM')"]) }} as member_month_sk
  , TO_CHAR(mc.claim_start_date, 'YYYYMM') as year_month
  , sc.service_category_sk
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
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('semantic_layer__stg_core__medical_claim') }} as mc
INNER JOIN {{ ref('semantic_layer__dim_service_category') }} as sc on mc.service_category_1 = sc.service_category_1
    AND mc.service_category_2 = sc.service_category_2
    AND mc.service_category_3 = sc.service_category_3
INNER JOIN encounter_sk esk on mc.encounter_id = esk.encounter_id
INNER JOIN {{ ref('semantic_layer__dim_data_source') }} as ds on mc.data_source = ds.data_source
WHERE enrollment_flag = 1