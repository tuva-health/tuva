{{ config(
     enabled = var('claims_enabled',False)
   )
}}

WITH CTE as (
SELECT * FROM {{ ref('data_quality__claim_allowed_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_charge_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_claim_id') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_claim_line_end_date') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_claim_line_number') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_claim_line_start_date') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_claim_type') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_coinsurance_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_copayment_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_data_source') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_deductible_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_diagnosis_code_type') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_hcpcs_code') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_member_id') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_paid_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_paid_date') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_patient_id') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_payer') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_plan') }}

UNION

SELECT * FROM {{ ref('data_quality__claim_total_cost_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_address') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_birth_date') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_city') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_data_source') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_death_date') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_death_flag') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_dual_status_code') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_end_date') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_first_name') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_gender') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_last_name') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_medicare_status_code') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_member_id') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_original_reason_entitlement_code') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_patient_id') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_payer_type') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_payer') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_phone') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_plan') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_race') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_start_date') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_state') }}

UNION

SELECT * FROM {{ ref('data_quality__eligibility_zip_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_ms_drg_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_admission_date') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_admit_source_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_admit_type_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_apr_drg_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_bill_type_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_billing_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_claim_end_date') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_claim_start_date') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_diagnosis_code_1') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_diagnosis_code_2') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_diagnosis_code_3') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_discharge_date') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_discharge_disposition_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_facility_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_present_on_admission_1') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_present_on_admission_2') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_present_on_admission_3') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_code_1') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_code_2') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_code_3') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_date_1') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_date_2') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_procedure_date_3') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_rendering_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_revenue_center_code') }}

UNION

SELECT * FROM {{ ref('data_quality__institutional_service_unit_quantity') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_allowed_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_claim_id') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_claim_line_number') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_coinsurance_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_copayment_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_data_source') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_days_supply') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_deductible_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_dispensing_date') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_dispensing_provider_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_member_id') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_ndc_code') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_paid_amount') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_paid_date') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_patient_id') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_payer') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_plan') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_prescribing_provider_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_quantity') }}

UNION

SELECT * FROM {{ ref('data_quality__pharmacy_refills') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_billing_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_facility_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_place_of_service_code') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_rendering_npi') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_claim_end_date') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_claim_start_date') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_diagnosis_code_1') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_diagnosis_code_2') }}

UNION

SELECT * FROM {{ ref('data_quality__professional_diagnosis_code_3') }}

)

SELECT *
,DENSE_RANK() OVER (ORDER BY DATA_SOURCE, TABLE_NAME, CLAIM_TYPE, FIELD_NAME) as SUMMARY_SK
FROM CTE