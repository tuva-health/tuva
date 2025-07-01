select
    surrogate_key
    , med.claim_id
    , med.claim_line_number
    , med.claim_type
    , med.person_id
    , med.member_id
    , med.payer
    , med.{{ quote_column('plan') }}
    , min(med.claim_start_date) over(partition by med.claim_id) as claim_start_date
    , max(med.claim_end_date) over(partition by med.claim_id) as claim_end_date
    , med.claim_line_start_date
    , med.claim_line_end_date
    , min(med.admission_date) over(partition by med.claim_id) as admission_date
    , max(med.discharge_date) over(partition by med.claim_id) as discharge_date
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
    , med.diagnosis_code_type
    , med.diagnosis_code_1
    , med.diagnosis_code_2
    , med.diagnosis_code_3
    , med.diagnosis_code_4
    , med.diagnosis_code_5
    , med.diagnosis_code_6
    , med.diagnosis_code_7
    , med.diagnosis_code_8
    , med.diagnosis_code_9
    , med.diagnosis_code_10
    , med.diagnosis_code_11
    , med.diagnosis_code_12
    , med.diagnosis_code_13
    , med.diagnosis_code_14
    , med.diagnosis_code_15
    , med.diagnosis_code_16
    , med.diagnosis_code_17
    , med.diagnosis_code_18
    , med.diagnosis_code_19
    , med.diagnosis_code_20
    , med.diagnosis_code_21
    , med.diagnosis_code_22
    , med.diagnosis_code_23
    , med.diagnosis_code_24
    , med.diagnosis_code_25
    , med.diagnosis_poa_1
    , med.diagnosis_poa_2
    , med.diagnosis_poa_3
    , med.diagnosis_poa_4
    , med.diagnosis_poa_5
    , med.diagnosis_poa_6
    , med.diagnosis_poa_7
    , med.diagnosis_poa_8
    , med.diagnosis_poa_9
    , med.diagnosis_poa_10
    , med.diagnosis_poa_11
    , med.diagnosis_poa_12
    , med.diagnosis_poa_13
    , med.diagnosis_poa_14
    , med.diagnosis_poa_15
    , med.diagnosis_poa_16
    , med.diagnosis_poa_17
    , med.diagnosis_poa_18
    , med.diagnosis_poa_19
    , med.diagnosis_poa_20
    , med.diagnosis_poa_21
    , med.diagnosis_poa_22
    , med.diagnosis_poa_23
    , med.diagnosis_poa_24
    , med.diagnosis_poa_25
    , med.procedure_code_type
    , med.procedure_code_1
    , med.procedure_code_2
    , med.procedure_code_3
    , med.procedure_code_4
    , med.procedure_code_5
    , med.procedure_code_6
    , med.procedure_code_7
    , med.procedure_code_8
    , med.procedure_code_9
    , med.procedure_code_10
    , med.procedure_code_11
    , med.procedure_code_12
    , med.procedure_code_13
    , med.procedure_code_14
    , med.procedure_code_15
    , med.procedure_code_16
    , med.procedure_code_17
    , med.procedure_code_18
    , med.procedure_code_19
    , med.procedure_code_20
    , med.procedure_code_21
    , med.procedure_code_22
    , med.procedure_code_23
    , med.procedure_code_24
    , med.procedure_code_25
    , med.procedure_date_1
    , med.procedure_date_2
    , med.procedure_date_3
    , med.procedure_date_4
    , med.procedure_date_5
    , med.procedure_date_6
    , med.procedure_date_7
    , med.procedure_date_8
    , med.procedure_date_9
    , med.procedure_date_10
    , med.procedure_date_11
    , med.procedure_date_12
    , med.procedure_date_13
    , med.procedure_date_14
    , med.procedure_date_15
    , med.procedure_date_16
    , med.procedure_date_17
    , med.procedure_date_18
    , med.procedure_date_19
    , med.procedure_date_20
    , med.procedure_date_21
    , med.procedure_date_22
    , med.procedure_date_23
    , med.procedure_date_24
    , med.procedure_date_25
    , med.in_network_flag
    , med.data_source
    , med.file_name
    , med.file_date
    , med.ingest_datetime
from {{ ref('the_tuva_project', 'normalized_input__stg_medical_claim') }} as med
    left outer join {{ ref('tuva_data_assets', 'npi') }} as rendering_prov
    on med.rendering_npi = rendering_prov.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as billing_prov
    on med.billing_npi = billing_prov.npi
    left outer join {{ ref('tuva_data_assets', 'npi') }} as facility_prov
    on med.facility_npi = facility_prov.npi