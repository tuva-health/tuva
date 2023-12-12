select
	med.claim_id
	, med.claim_line_number
	, med.claim_type
	, med.patient_id
	, med.member_id
	, med.payer
	, med.plan 
	, dates.minimum_claim_start_date as claim_start_date
	, dates.maximum_claim_end_date as claim_end_date
	, med.claim_line_start_date
	, med.claim_line_end_date
	, dates.minimum_admission_date as admission_date
	, dates.maximum_discharge_date as discharge_date
	, coalesce(ad_source.normalized_code, other.admit_source_code) as admit_source_code
	, coalesce(ad_type.normalized_code, other.admit_type_code) as admit_type_code
	, coalesce(disch_disp.normalized_code, other.discharge_disposition_code) as discharge_disposition_code
	, pos.normalized_code as place_of_service_code
	, coalesce(bill.normalized_code, other.bill_type_code) as bill_type_code
	, coalesce(ms.normalized_code, other.ms_drg_code) as ms_drg_code
	, coalesce(apr.normalized_code, other.apr_drg_code) as apr_drg_code
	, rev.normalized_code as revenue_center_code
	, med.service_unit_quantity
	, med.hcpcs_code
	, med.hcpcs_modifier_1
	, med.hcpcs_modifier_2
	, med.hcpcs_modifier_3
	, med.hcpcs_modifier_4
	, med.hcpcs_modifier_5
	, med.rendering_npi
	, med.billing_npi
	, med.facility_npi
	, med.paid_date
	, med.paid_amount
	, med.allowed_amount
	, med.charge_amount
	, med.coinsurance_amount
	, med.copayment_amount
	, med.deductible_amount
	, med.total_cost_amount
	, med.diagnosis_code_type
	, coalesce(dx_code.diagnosis_code_1, other.diagnosis_code_1) as diagnosis_code_1
	, coalesce(dx_code.diagnosis_code_2, other.diagnosis_code_2) as diagnosis_code_2
	, coalesce(dx_code.diagnosis_code_3, other.diagnosis_code_3) as diagnosis_code_3
	, coalesce(dx_code.diagnosis_code_4, other.diagnosis_code_4) as diagnosis_code_4
	, coalesce(dx_code.diagnosis_code_5, other.diagnosis_code_5) as diagnosis_code_5
	, coalesce(dx_code.diagnosis_code_6, other.diagnosis_code_6) as diagnosis_code_6
	, coalesce(dx_code.diagnosis_code_7, other.diagnosis_code_7) as diagnosis_code_7
	, coalesce(dx_code.diagnosis_code_8, other.diagnosis_code_8) as diagnosis_code_8
	, coalesce(dx_code.diagnosis_code_9, other.diagnosis_code_9) as diagnosis_code_9
	, coalesce(dx_code.diagnosis_code_10, other.diagnosis_code_10) as diagnosis_code_10
	, coalesce(dx_code.diagnosis_code_11, other.diagnosis_code_11) as diagnosis_code_11
	, coalesce(dx_code.diagnosis_code_12, other.diagnosis_code_12) as diagnosis_code_12
	, coalesce(dx_code.diagnosis_code_13, other.diagnosis_code_13) as diagnosis_code_13
	, coalesce(dx_code.diagnosis_code_14, other.diagnosis_code_14) as diagnosis_code_14
	, coalesce(dx_code.diagnosis_code_15, other.diagnosis_code_15) as diagnosis_code_15
	, coalesce(dx_code.diagnosis_code_16, other.diagnosis_code_16) as diagnosis_code_16
	, coalesce(dx_code.diagnosis_code_17, other.diagnosis_code_17) as diagnosis_code_17
	, coalesce(dx_code.diagnosis_code_18, other.diagnosis_code_18) as diagnosis_code_18
	, coalesce(dx_code.diagnosis_code_19, other.diagnosis_code_19) as diagnosis_code_19
	, coalesce(dx_code.diagnosis_code_20, other.diagnosis_code_20) as diagnosis_code_20
	, coalesce(dx_code.diagnosis_code_21, other.diagnosis_code_21) as diagnosis_code_21
	, coalesce(dx_code.diagnosis_code_22, other.diagnosis_code_22) as diagnosis_code_22
	, coalesce(dx_code.diagnosis_code_23, other.diagnosis_code_23) as diagnosis_code_23
	, coalesce(dx_code.diagnosis_code_24, other.diagnosis_code_24) as diagnosis_code_24
	, coalesce(dx_code.diagnosis_code_25, other.diagnosis_code_25) as diagnosis_code_25
	, coalesce(poa.diagnosis_poa_1, other.diagnosis_poa_1) as diagnosis_poa_1
	, coalesce(poa.diagnosis_poa_2, other.diagnosis_poa_2) as diagnosis_poa_2
	, coalesce(poa.diagnosis_poa_3, other.diagnosis_poa_3) as diagnosis_poa_3
	, coalesce(poa.diagnosis_poa_4, other.diagnosis_poa_4) as diagnosis_poa_4
	, coalesce(poa.diagnosis_poa_5, other.diagnosis_poa_5) as diagnosis_poa_5
	, coalesce(poa.diagnosis_poa_6, other.diagnosis_poa_6) as diagnosis_poa_6
	, coalesce(poa.diagnosis_poa_7, other.diagnosis_poa_7) as diagnosis_poa_7
	, coalesce(poa.diagnosis_poa_8, other.diagnosis_poa_8) as diagnosis_poa_8
	, coalesce(poa.diagnosis_poa_9, other.diagnosis_poa_9) as diagnosis_poa_9
	, coalesce(poa.diagnosis_poa_10, other.diagnosis_poa_10) as diagnosis_poa_10
	, coalesce(poa.diagnosis_poa_11, other.diagnosis_poa_11) as diagnosis_poa_11
	, coalesce(poa.diagnosis_poa_12, other.diagnosis_poa_12) as diagnosis_poa_12
	, coalesce(poa.diagnosis_poa_13, other.diagnosis_poa_13) as diagnosis_poa_13
	, coalesce(poa.diagnosis_poa_14, other.diagnosis_poa_14) as diagnosis_poa_14
	, coalesce(poa.diagnosis_poa_15, other.diagnosis_poa_15) as diagnosis_poa_15
	, coalesce(poa.diagnosis_poa_16, other.diagnosis_poa_16) as diagnosis_poa_16
	, coalesce(poa.diagnosis_poa_17, other.diagnosis_poa_17) as diagnosis_poa_17
	, coalesce(poa.diagnosis_poa_18, other.diagnosis_poa_18) as diagnosis_poa_18
	, coalesce(poa.diagnosis_poa_19, other.diagnosis_poa_19) as diagnosis_poa_19
	, coalesce(poa.diagnosis_poa_20, other.diagnosis_poa_20) as diagnosis_poa_20
	, coalesce(poa.diagnosis_poa_21, other.diagnosis_poa_21) as diagnosis_poa_21
	, coalesce(poa.diagnosis_poa_22, other.diagnosis_poa_22) as diagnosis_poa_22
	, coalesce(poa.diagnosis_poa_23, other.diagnosis_poa_23) as diagnosis_poa_23
	, coalesce(poa.diagnosis_poa_24, other.diagnosis_poa_24) as diagnosis_poa_24
	, coalesce(poa.diagnosis_poa_25, other.diagnosis_poa_25) as diagnosis_poa_25
	, med.procedure_code_type
	, coalesce(px_code.procedure_code_1, other.procedure_code_1) as procedure_code_1
	, coalesce(px_code.procedure_code_2, other.procedure_code_2) as procedure_code_2
	, coalesce(px_code.procedure_code_3, other.procedure_code_3) as procedure_code_3
	, coalesce(px_code.procedure_code_4, other.procedure_code_4) as procedure_code_4
	, coalesce(px_code.procedure_code_5, other.procedure_code_5) as procedure_code_5
	, coalesce(px_code.procedure_code_6, other.procedure_code_6) as procedure_code_6
	, coalesce(px_code.procedure_code_7, other.procedure_code_7) as procedure_code_7
	, coalesce(px_code.procedure_code_8, other.procedure_code_8) as procedure_code_8
	, coalesce(px_code.procedure_code_9, other.procedure_code_9) as procedure_code_9
	, coalesce(px_code.procedure_code_10, other.procedure_code_10) as procedure_code_10
	, coalesce(px_code.procedure_code_11, other.procedure_code_11) as procedure_code_11
	, coalesce(px_code.procedure_code_12, other.procedure_code_12) as procedure_code_12
	, coalesce(px_code.procedure_code_13, other.procedure_code_13) as procedure_code_13
	, coalesce(px_code.procedure_code_14, other.procedure_code_14) as procedure_code_14
	, coalesce(px_code.procedure_code_15, other.procedure_code_15) as procedure_code_15
	, coalesce(px_code.procedure_code_16, other.procedure_code_16) as procedure_code_16
	, coalesce(px_code.procedure_code_17, other.procedure_code_17) as procedure_code_17
	, coalesce(px_code.procedure_code_18, other.procedure_code_18) as procedure_code_18
	, coalesce(px_code.procedure_code_19, other.procedure_code_19) as procedure_code_19
	, coalesce(px_code.procedure_code_20, other.procedure_code_20) as procedure_code_20
	, coalesce(px_code.procedure_code_21, other.procedure_code_21) as procedure_code_21
	, coalesce(px_code.procedure_code_22, other.procedure_code_22) as procedure_code_22
	, coalesce(px_code.procedure_code_23, other.procedure_code_23) as procedure_code_23
	, coalesce(px_code.procedure_code_24, other.procedure_code_24) as procedure_code_24
	, coalesce(px_code.procedure_code_25, other.procedure_code_25) as procedure_code_25
	, coalesce(px_date.procedure_date_1, other.procedure_date_1) as procedure_date_1
	, coalesce(px_date.procedure_date_2, other.procedure_date_2) as procedure_date_2
	, coalesce(px_date.procedure_date_3, other.procedure_date_3) as procedure_date_3
	, coalesce(px_date.procedure_date_4, other.procedure_date_4) as procedure_date_4
	, coalesce(px_date.procedure_date_5, other.procedure_date_5) as procedure_date_5
	, coalesce(px_date.procedure_date_6, other.procedure_date_6) as procedure_date_6
	, coalesce(px_date.procedure_date_7, other.procedure_date_7) as procedure_date_7
	, coalesce(px_date.procedure_date_8, other.procedure_date_8) as procedure_date_8
	, coalesce(px_date.procedure_date_9, other.procedure_date_9) as procedure_date_9
	, coalesce(px_date.procedure_date_10, other.procedure_date_10) as procedure_date_10
	, coalesce(px_date.procedure_date_11, other.procedure_date_11) as procedure_date_11
	, coalesce(px_date.procedure_date_12, other.procedure_date_12) as procedure_date_12
	, coalesce(px_date.procedure_date_13, other.procedure_date_13) as procedure_date_13
	, coalesce(px_date.procedure_date_14, other.procedure_date_14) as procedure_date_14
	, coalesce(px_date.procedure_date_15, other.procedure_date_15) as procedure_date_15
	, coalesce(px_date.procedure_date_16, other.procedure_date_16) as procedure_date_16
	, coalesce(px_date.procedure_date_17, other.procedure_date_17) as procedure_date_17
	, coalesce(px_date.procedure_date_18, other.procedure_date_18) as procedure_date_18
	, coalesce(px_date.procedure_date_19, other.procedure_date_19) as procedure_date_19
	, coalesce(px_date.procedure_date_20, other.procedure_date_20) as procedure_date_20
	, coalesce(px_date.procedure_date_21, other.procedure_date_21) as procedure_date_21
	, coalesce(px_date.procedure_date_22, other.procedure_date_22) as procedure_date_22
	, coalesce(px_date.procedure_date_23, other.procedure_date_23) as procedure_date_23
	, coalesce(px_date.procedure_date_24, other.procedure_date_24) as procedure_date_24
	, coalesce(px_date.procedure_date_25, other.procedure_date_25) as procedure_date_25
	, med.data_source
from {{ ref('medical_claim') }} med
left join {{ref('normalized_input__int_admit_source_final') }} ad_source
    on med.claim_id = ad_source.claim_id
    and med.data_source = ad_source.data_source
left join {{ref('normalized_input__int_admit_type_final') }} ad_type
    on med.claim_id = ad_type.claim_id
    and med.data_source = ad_type.data_source
left join {{ref('normalized_input__int_apr_drg_final') }} apr
    on med.claim_id = apr.claim_id
    and med.data_source = apr.data_source
left join {{ref('normalized_input__int_bill_type_final') }} bill
    on med.claim_id = bill.claim_id
    and med.data_source = bill.data_source
left join {{ref('normalized_input__int_medical_date_aggregation') }} dates
    on med.claim_id = dates.claim_id
    and med.data_source = dates.data_source
left join {{ref('normalized_input__int_discharge_disposition_final') }} disch_disp
    on med.claim_id = disch_disp.claim_id
    and med.data_source = disch_disp.data_source
left join {{ref('normalized_input__int_ms_drg_final') }} ms
    on med.claim_id = ms.claim_id
    and med.data_source = ms.data_source
left join {{ref('normalized_input__int_place_of_service_normalize') }} pos
    on med.claim_id = pos.claim_id
    and med.claim_line_number = pos.claim_line_number
    and med.data_source = pos.data_source
left join {{ref('normalized_input__int_diagnosis_code_final') }} dx_code
    on med.claim_id = dx_code.claim_id
    and med.data_source = dx_code.data_source
left join {{ref('normalized_input__int_present_on_admit_final') }} poa
    on med.claim_id = poa.claim_id
    and med.data_source = poa.data_source
left join {{ref('normalized_input__int_procedure_code_final') }} px_code
    on med.claim_id = px_code.claim_id
    and med.data_source = px_code.data_source
left join {{ref('normalized_input__int_procedure_date_final') }} px_date
    on med.claim_id = px_date.claim_id
    and med.data_source = px_date.data_source
left join {{ref('normalized_input__int_revenue_center_normalize') }} rev
    on med.claim_id = rev.claim_id
    and med.claim_line_number = rev.claim_line_number
    and med.data_source = rev.data_source
left join {{ref('normalized_input__int_undetermined_claim_type') }} other
    on med.claim_id = other.claim_id
    and med.data_source = other.data_source

