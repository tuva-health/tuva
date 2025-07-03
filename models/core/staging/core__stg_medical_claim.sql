select
    med.medical_claim_sk
    , med.data_source
    , med.claim_id
    , med.claim_line_number
    , med.claim_type
    , med.person_id
    , med.member_id
    , med.payer
    , med.{{ quote_column('plan') }}
    , med.claim_start_date
    , med.claim_end_dateclaim_end_date
    , med.claim_line_start_date
    , med.claim_line_end_date
    , med.admission_date
    , med.discharge_date
    , med.admit_source_code
    , med.admit_type_code
    , med.discharge_disposition_code
    , med.place_of_service_code
    , med.bill_type_code
    , med.drg_code_type
    , med.drg_code
    , med.revenue_center_code
    , med.service_unit_quantity
    , med.hcpcs_code
    , med.hcpcs_modifier_1
    , med.hcpcs_modifier_2
    , med.hcpcs_modifier_3
    , med.hcpcs_modifier_4
    , med.hcpcs_modifier_5
    , med.rendering_npi
    , rendering_prov.provider_name as rendering_name
    , med.rendering_tin
    , med.billing_npi
    , billing_prov.provider_name as billing_name
    , med.billing_tin
    , med.facility_npi
    , facility_prov.provider_name as facility_name
    , med.paid_date
    , med.paid_amount
    , med.allowed_amount
    , med.charge_amount
    , med.coinsurance_amount
    , med.copayment_amount
    , med.deductible_amount
    , med.total_cost_amount
    , in_network_flag
from {{ ref('the_tuva_project', 'normalized_input__medical_claim') }} as med
    left outer join {{ ref('tuva_data_assets', 'npi') }} as rendering_prov
    on med.rendering_npi = rendering_prov.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as billing_prov
    on med.billing_npi = billing_prov.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as facility_prov
    on med.facility_npi = facility_prov.npi
    -- TODO: Get service category groupers and enrollment flag