{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select
	cast(med.claim_id as {{ dbt.type_string() }} ) as claim_id
	, cast(med.claim_line_number as int ) as claim_line_number
	, cast(med.claim_type as {{ dbt.type_string() }} ) as claim_type
	, cast(med.patient_id as {{ dbt.type_string() }} ) as patient_id
	, cast(med.member_id as {{ dbt.type_string() }} ) as member_id
	, cast(med.payer as {{ dbt.type_string() }} ) as payer
	, cast(med.plan as {{ dbt.type_string() }} ) as plan
	, cast(coalesce(dates.minimum_claim_start_date, undetermined.claim_start_date) as date ) as claim_start_date
	, cast(coalesce(dates.maximum_claim_end_date, undetermined.claim_start_date) as date ) as claim_end_date
	, cast(coalesce(claim_line_dates.normalized_claim_line_start_date, undetermined.claim_line_start_date) as date ) as claim_line_start_date
	, cast(coalesce(claim_line_dates.normalized_claim_line_end_date, undetermined.claim_line_end_date) as date ) as claim_line_end_date
	, cast(coalesce(dates.minimum_admission_date, undetermined.admission_date) as date ) as admission_date
	, cast(coalesce(dates.maximum_discharge_date, undetermined.discharge_date) as date ) as discharge_date
	, cast(coalesce(ad_source.normalized_code, undetermined.admit_source_code) as {{ dbt.type_string() }} ) as admit_source_code
	, cast(coalesce(ad_type.normalized_code, undetermined.admit_type_code) as {{ dbt.type_string() }} ) as admit_type_code
	, cast(coalesce(disch_disp.normalized_code, undetermined.discharge_disposition_code) as {{ dbt.type_string() }} ) as discharge_disposition_code
	, cast(coalesce(pos.normalized_code, undetermined.place_of_service_code) as {{ dbt.type_string() }} ) as place_of_service_code
	, cast(coalesce(bill.normalized_code, undetermined.bill_type_code) as {{ dbt.type_string() }} ) as bill_type_code
	, cast(coalesce(ms.normalized_code, undetermined.ms_drg_code) as {{ dbt.type_string() }} ) as ms_drg_code
	, cast(coalesce(apr.normalized_code, undetermined.apr_drg_code) as {{ dbt.type_string() }} ) as apr_drg_code
	, cast(coalesce(rev.normalized_code, undetermined.revenue_center_code) as {{ dbt.type_string() }} ) as revenue_center_code
	, cast(med.service_unit_quantity as {{ dbt.type_string() }} ) as service_unit_quantity
	, cast(med.hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
	, cast(med.hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
	, cast(med.hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
	, cast(med.hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
	, cast(med.hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
	, cast(med.hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
	, cast(coalesce(med_npi.normalized_rendering_npi, undetermined.rendering_npi) as {{ dbt.type_string() }} ) as rendering_npi
	, cast(coalesce(med_npi.normalized_billing_npi, undetermined.billing_npi) as {{ dbt.type_string() }} ) as billing_npi
	, cast(coalesce(med_npi.normalized_facility_npi, undetermined.facility_npi) as {{ dbt.type_string() }} ) as facility_npi
	, cast(med.paid_date as date ) as paid_date
	, cast(med.paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
	, cast(med.allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
	, cast(med.charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
	, cast(med.coinsurance_amount as {{ dbt.type_numeric() }} ) as coinsurance_amount
	, cast(med.copayment_amount as {{ dbt.type_numeric() }} ) as copayment_amount
	, cast(med.deductible_amount as {{ dbt.type_numeric() }} ) as deductible_amount
	, cast(med.total_cost_amount as {{ dbt.type_numeric() }} ) as total_cost_amount
	, cast(med.diagnosis_code_type as {{ dbt.type_string() }} ) as diagnosis_code_type
	, cast(coalesce(dx_code.diagnosis_code_1, undetermined.diagnosis_code_1) as {{ dbt.type_string() }} ) as diagnosis_code_1
	, cast(coalesce(dx_code.diagnosis_code_2, undetermined.diagnosis_code_2) as {{ dbt.type_string() }} ) as diagnosis_code_2
	, cast(coalesce(dx_code.diagnosis_code_3, undetermined.diagnosis_code_3) as {{ dbt.type_string() }} ) as diagnosis_code_3
	, cast(coalesce(dx_code.diagnosis_code_4, undetermined.diagnosis_code_4) as {{ dbt.type_string() }} ) as diagnosis_code_4
	, cast(coalesce(dx_code.diagnosis_code_5, undetermined.diagnosis_code_5) as {{ dbt.type_string() }} ) as diagnosis_code_5
	, cast(coalesce(dx_code.diagnosis_code_6, undetermined.diagnosis_code_6) as {{ dbt.type_string() }} ) as diagnosis_code_6
	, cast(coalesce(dx_code.diagnosis_code_7, undetermined.diagnosis_code_7) as {{ dbt.type_string() }} ) as diagnosis_code_7
	, cast(coalesce(dx_code.diagnosis_code_8, undetermined.diagnosis_code_8) as {{ dbt.type_string() }} ) as diagnosis_code_8
	, cast(coalesce(dx_code.diagnosis_code_9, undetermined.diagnosis_code_9) as {{ dbt.type_string() }} ) as diagnosis_code_9
	, cast(coalesce(dx_code.diagnosis_code_10, undetermined.diagnosis_code_10) as {{ dbt.type_string() }} ) as diagnosis_code_10
	, cast(coalesce(dx_code.diagnosis_code_11, undetermined.diagnosis_code_11) as {{ dbt.type_string() }} ) as diagnosis_code_11
	, cast(coalesce(dx_code.diagnosis_code_12, undetermined.diagnosis_code_12) as {{ dbt.type_string() }} ) as diagnosis_code_12
	, cast(coalesce(dx_code.diagnosis_code_13, undetermined.diagnosis_code_13) as {{ dbt.type_string() }} ) as diagnosis_code_13
	, cast(coalesce(dx_code.diagnosis_code_14, undetermined.diagnosis_code_14) as {{ dbt.type_string() }} ) as diagnosis_code_14
	, cast(coalesce(dx_code.diagnosis_code_15, undetermined.diagnosis_code_15) as {{ dbt.type_string() }} ) as diagnosis_code_15
	, cast(coalesce(dx_code.diagnosis_code_16, undetermined.diagnosis_code_16) as {{ dbt.type_string() }} ) as diagnosis_code_16
	, cast(coalesce(dx_code.diagnosis_code_17, undetermined.diagnosis_code_17) as {{ dbt.type_string() }} ) as diagnosis_code_17
	, cast(coalesce(dx_code.diagnosis_code_18, undetermined.diagnosis_code_18) as {{ dbt.type_string() }} ) as diagnosis_code_18
	, cast(coalesce(dx_code.diagnosis_code_19, undetermined.diagnosis_code_19) as {{ dbt.type_string() }} ) as diagnosis_code_19
	, cast(coalesce(dx_code.diagnosis_code_20, undetermined.diagnosis_code_20) as {{ dbt.type_string() }} ) as diagnosis_code_20
	, cast(coalesce(dx_code.diagnosis_code_21, undetermined.diagnosis_code_21) as {{ dbt.type_string() }} ) as diagnosis_code_21
	, cast(coalesce(dx_code.diagnosis_code_22, undetermined.diagnosis_code_22) as {{ dbt.type_string() }} ) as diagnosis_code_22
	, cast(coalesce(dx_code.diagnosis_code_23, undetermined.diagnosis_code_23) as {{ dbt.type_string() }} ) as diagnosis_code_23
	, cast(coalesce(dx_code.diagnosis_code_24, undetermined.diagnosis_code_24) as {{ dbt.type_string() }} ) as diagnosis_code_24
	, cast(coalesce(dx_code.diagnosis_code_25, undetermined.diagnosis_code_25) as {{ dbt.type_string() }} ) as diagnosis_code_25
	, cast(coalesce(poa.diagnosis_poa_1, undetermined.diagnosis_poa_1) as {{ dbt.type_string() }} ) as diagnosis_poa_1
	, cast(coalesce(poa.diagnosis_poa_2, undetermined.diagnosis_poa_2) as {{ dbt.type_string() }} ) as diagnosis_poa_2
	, cast(coalesce(poa.diagnosis_poa_3, undetermined.diagnosis_poa_3) as {{ dbt.type_string() }} ) as diagnosis_poa_3
	, cast(coalesce(poa.diagnosis_poa_4, undetermined.diagnosis_poa_4) as {{ dbt.type_string() }} ) as diagnosis_poa_4
	, cast(coalesce(poa.diagnosis_poa_5, undetermined.diagnosis_poa_5) as {{ dbt.type_string() }} ) as diagnosis_poa_5
	, cast(coalesce(poa.diagnosis_poa_6, undetermined.diagnosis_poa_6) as {{ dbt.type_string() }} ) as diagnosis_poa_6
	, cast(coalesce(poa.diagnosis_poa_7, undetermined.diagnosis_poa_7) as {{ dbt.type_string() }} ) as diagnosis_poa_7
	, cast(coalesce(poa.diagnosis_poa_8, undetermined.diagnosis_poa_8) as {{ dbt.type_string() }} ) as diagnosis_poa_8
	, cast(coalesce(poa.diagnosis_poa_9, undetermined.diagnosis_poa_9) as {{ dbt.type_string() }} ) as diagnosis_poa_9
	, cast(coalesce(poa.diagnosis_poa_10, undetermined.diagnosis_poa_10) as {{ dbt.type_string() }} ) as diagnosis_poa_10
	, cast(coalesce(poa.diagnosis_poa_11, undetermined.diagnosis_poa_11) as {{ dbt.type_string() }} ) as diagnosis_poa_11
	, cast(coalesce(poa.diagnosis_poa_12, undetermined.diagnosis_poa_12) as {{ dbt.type_string() }} ) as diagnosis_poa_12
	, cast(coalesce(poa.diagnosis_poa_13, undetermined.diagnosis_poa_13) as {{ dbt.type_string() }} ) as diagnosis_poa_13
	, cast(coalesce(poa.diagnosis_poa_14, undetermined.diagnosis_poa_14) as {{ dbt.type_string() }} ) as diagnosis_poa_14
	, cast(coalesce(poa.diagnosis_poa_15, undetermined.diagnosis_poa_15) as {{ dbt.type_string() }} ) as diagnosis_poa_15
	, cast(coalesce(poa.diagnosis_poa_16, undetermined.diagnosis_poa_16) as {{ dbt.type_string() }} ) as diagnosis_poa_16
	, cast(coalesce(poa.diagnosis_poa_17, undetermined.diagnosis_poa_17) as {{ dbt.type_string() }} ) as diagnosis_poa_17
	, cast(coalesce(poa.diagnosis_poa_18, undetermined.diagnosis_poa_18) as {{ dbt.type_string() }} ) as diagnosis_poa_18
	, cast(coalesce(poa.diagnosis_poa_19, undetermined.diagnosis_poa_19) as {{ dbt.type_string() }} ) as diagnosis_poa_19
	, cast(coalesce(poa.diagnosis_poa_20, undetermined.diagnosis_poa_20) as {{ dbt.type_string() }} ) as diagnosis_poa_20
	, cast(coalesce(poa.diagnosis_poa_21, undetermined.diagnosis_poa_21) as {{ dbt.type_string() }} ) as diagnosis_poa_21
	, cast(coalesce(poa.diagnosis_poa_22, undetermined.diagnosis_poa_22) as {{ dbt.type_string() }} ) as diagnosis_poa_22
	, cast(coalesce(poa.diagnosis_poa_23, undetermined.diagnosis_poa_23) as {{ dbt.type_string() }} ) as diagnosis_poa_23
	, cast(coalesce(poa.diagnosis_poa_24, undetermined.diagnosis_poa_24) as {{ dbt.type_string() }} ) as diagnosis_poa_24
	, cast(coalesce(poa.diagnosis_poa_25, undetermined.diagnosis_poa_25) as {{ dbt.type_string() }} ) as diagnosis_poa_25
	, cast(med.procedure_code_type as {{ dbt.type_string() }} ) as procedure_code_type
	, cast(coalesce(px_code.procedure_code_1, undetermined.procedure_code_1) as {{ dbt.type_string() }} ) as procedure_code_1
	, cast(coalesce(px_code.procedure_code_2, undetermined.procedure_code_2) as {{ dbt.type_string() }} ) as procedure_code_2
	, cast(coalesce(px_code.procedure_code_3, undetermined.procedure_code_3) as {{ dbt.type_string() }} ) as procedure_code_3
	, cast(coalesce(px_code.procedure_code_4, undetermined.procedure_code_4) as {{ dbt.type_string() }} ) as procedure_code_4
	, cast(coalesce(px_code.procedure_code_5, undetermined.procedure_code_5) as {{ dbt.type_string() }} ) as procedure_code_5
	, cast(coalesce(px_code.procedure_code_6, undetermined.procedure_code_6) as {{ dbt.type_string() }} ) as procedure_code_6
	, cast(coalesce(px_code.procedure_code_7, undetermined.procedure_code_7) as {{ dbt.type_string() }} ) as procedure_code_7
	, cast(coalesce(px_code.procedure_code_8, undetermined.procedure_code_8) as {{ dbt.type_string() }} ) as procedure_code_8
	, cast(coalesce(px_code.procedure_code_9, undetermined.procedure_code_9) as {{ dbt.type_string() }} ) as procedure_code_9
	, cast(coalesce(px_code.procedure_code_10, undetermined.procedure_code_10) as {{ dbt.type_string() }} ) as procedure_code_10
	, cast(coalesce(px_code.procedure_code_11, undetermined.procedure_code_11) as {{ dbt.type_string() }} ) as procedure_code_11
	, cast(coalesce(px_code.procedure_code_12, undetermined.procedure_code_12) as {{ dbt.type_string() }} ) as procedure_code_12
	, cast(coalesce(px_code.procedure_code_13, undetermined.procedure_code_13) as {{ dbt.type_string() }} ) as procedure_code_13
	, cast(coalesce(px_code.procedure_code_14, undetermined.procedure_code_14) as {{ dbt.type_string() }} ) as procedure_code_14
	, cast(coalesce(px_code.procedure_code_15, undetermined.procedure_code_15) as {{ dbt.type_string() }} ) as procedure_code_15
	, cast(coalesce(px_code.procedure_code_16, undetermined.procedure_code_16) as {{ dbt.type_string() }} ) as procedure_code_16
	, cast(coalesce(px_code.procedure_code_17, undetermined.procedure_code_17) as {{ dbt.type_string() }} ) as procedure_code_17
	, cast(coalesce(px_code.procedure_code_18, undetermined.procedure_code_18) as {{ dbt.type_string() }} ) as procedure_code_18
	, cast(coalesce(px_code.procedure_code_19, undetermined.procedure_code_19) as {{ dbt.type_string() }} ) as procedure_code_19
	, cast(coalesce(px_code.procedure_code_20, undetermined.procedure_code_20) as {{ dbt.type_string() }} ) as procedure_code_20
	, cast(coalesce(px_code.procedure_code_21, undetermined.procedure_code_21) as {{ dbt.type_string() }} ) as procedure_code_21
	, cast(coalesce(px_code.procedure_code_22, undetermined.procedure_code_22) as {{ dbt.type_string() }} ) as procedure_code_22
	, cast(coalesce(px_code.procedure_code_23, undetermined.procedure_code_23) as {{ dbt.type_string() }} ) as procedure_code_23
	, cast(coalesce(px_code.procedure_code_24, undetermined.procedure_code_24) as {{ dbt.type_string() }} ) as procedure_code_24
	, cast(coalesce(px_code.procedure_code_25, undetermined.procedure_code_25) as {{ dbt.type_string() }} ) as procedure_code_25
	, cast(coalesce(px_date.procedure_date_1, undetermined.procedure_date_1) as date ) as procedure_date_1
	, cast(coalesce(px_date.procedure_date_2, undetermined.procedure_date_2) as date ) as procedure_date_2
	, cast(coalesce(px_date.procedure_date_3, undetermined.procedure_date_3) as date ) as procedure_date_3
	, cast(coalesce(px_date.procedure_date_4, undetermined.procedure_date_4) as date ) as procedure_date_4
	, cast(coalesce(px_date.procedure_date_5, undetermined.procedure_date_5) as date ) as procedure_date_5
	, cast(coalesce(px_date.procedure_date_6, undetermined.procedure_date_6) as date ) as procedure_date_6
	, cast(coalesce(px_date.procedure_date_7, undetermined.procedure_date_7) as date ) as procedure_date_7
	, cast(coalesce(px_date.procedure_date_8, undetermined.procedure_date_8) as date ) as procedure_date_8
	, cast(coalesce(px_date.procedure_date_9, undetermined.procedure_date_9) as date ) as procedure_date_9
	, cast(coalesce(px_date.procedure_date_10, undetermined.procedure_date_10) as date ) as procedure_date_10
	, cast(coalesce(px_date.procedure_date_11, undetermined.procedure_date_11) as date ) as procedure_date_11
	, cast(coalesce(px_date.procedure_date_12, undetermined.procedure_date_12) as date ) as procedure_date_12
	, cast(coalesce(px_date.procedure_date_13, undetermined.procedure_date_13) as date ) as procedure_date_13
	, cast(coalesce(px_date.procedure_date_14, undetermined.procedure_date_14) as date ) as procedure_date_14
	, cast(coalesce(px_date.procedure_date_15, undetermined.procedure_date_15) as date ) as procedure_date_15
	, cast(coalesce(px_date.procedure_date_16, undetermined.procedure_date_16) as date ) as procedure_date_16
	, cast(coalesce(px_date.procedure_date_17, undetermined.procedure_date_17) as date ) as procedure_date_17
	, cast(coalesce(px_date.procedure_date_18, undetermined.procedure_date_18) as date ) as procedure_date_18
	, cast(coalesce(px_date.procedure_date_19, undetermined.procedure_date_19) as date ) as procedure_date_19
	, cast(coalesce(px_date.procedure_date_20, undetermined.procedure_date_20) as date ) as procedure_date_20
	, cast(coalesce(px_date.procedure_date_21, undetermined.procedure_date_21) as date ) as procedure_date_21
	, cast(coalesce(px_date.procedure_date_22, undetermined.procedure_date_22) as date ) as procedure_date_22
	, cast(coalesce(px_date.procedure_date_23, undetermined.procedure_date_23) as date ) as procedure_date_23
	, cast(coalesce(px_date.procedure_date_24, undetermined.procedure_date_24) as date ) as procedure_date_24
	, cast(coalesce(px_date.procedure_date_25, undetermined.procedure_date_25) as date ) as procedure_date_25
	, cast(med.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_string() }} ) as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
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
left join {{ref('normalized_input__int_medical_claim_date_normalize') }} claim_line_dates
    on med.claim_id = claim_line_dates.claim_id
    and med.claim_line_number = claim_line_dates.claim_line_number
    and med.data_source = claim_line_dates.data_source
left join {{ref('normalized_input__int_medical_date_aggregation') }} dates
    on med.claim_id = dates.claim_id
    and med.data_source = dates.data_source
left join {{ref('normalized_input__int_medical_npi_normalize') }} med_npi
    on med.claim_id = med_npi.claim_id
    and med.claim_line_number = med_npi.claim_line_number
    and med.data_source = med_npi.data_source
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
left join {{ref('normalized_input__int_undetermined_claim_type') }} undetermined
    on med.claim_id = undetermined.claim_id
    and med.data_source = undetermined.data_source

