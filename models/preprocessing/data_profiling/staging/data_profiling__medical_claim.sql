{{ config(
     enabled = var('data_profiling_enabled',var('tuva_packages_enabled',True))
   )
}}


select
	  nullif(claim_id, '') as claim_id
    , claim_line_number as claim_line_number
    , nullif(claim_type, '') as claim_type
    , nullif(patient_id, '') as patient_id
    , nullif(member_id, '') as member_id
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , admission_date
    , discharge_date
    , nullif(admit_source_code, '') as admit_source_code
    , nullif(admit_type_code, '') as admit_type_code
    , nullif(discharge_disposition_code, '') as discharge_disposition_code
    , nullif(place_of_service_code, '') as place_of_service_code
    , nullif(bill_type_code, '') as bill_type_code
    , nullif(ms_drg_code, '') as ms_drg_code 
    , nullif(apr_drg_code, '') as apr_drg_code 
    , nullif(revenue_center_code, '') as revenue_center_code
    , service_unit_quantity
    , nullif(hcpcs_code, '') as hcpcs_code
    , nullif(hcpcs_modifier_1, '') as hcpcs_modifier_1
    , nullif(hcpcs_modifier_2, '') as hcpcs_modifier_2
    , nullif(hcpcs_modifier_3, '') as hcpcs_modifier_3
    , nullif(hcpcs_modifier_4, '') as hcpcs_modifier_4
    , nullif(hcpcs_modifier_5, '') as hcpcs_modifier_5
    , nullif(rendering_npi, '') as rendering_npi
    , nullif(billing_npi, '') as billing_npi
    , nullif(facility_npi, '') as facility_npi
    , paid_date
    , paid_amount
    , allowed_amount
    , charge_amount
    , nullif(diagnosis_code_type, '') as diagnosis_code_type
    , nullif(diagnosis_code_1, '') as diagnosis_code_1
    , nullif(diagnosis_code_2, '') as diagnosis_code_2
    , nullif(diagnosis_code_3, '') as diagnosis_code_3
    , nullif(diagnosis_code_4, '') as diagnosis_code_4
    , nullif(diagnosis_code_5, '') as diagnosis_code_5
    , nullif(diagnosis_code_6, '') as diagnosis_code_6
    , nullif(diagnosis_code_7, '') as diagnosis_code_7
    , nullif(diagnosis_code_8, '') as diagnosis_code_8
    , nullif(diagnosis_code_9, '') as diagnosis_code_9
    , nullif(diagnosis_code_10, '') as diagnosis_code_10
    , nullif(diagnosis_code_11, '') as diagnosis_code_11
    , nullif(diagnosis_code_12, '') as diagnosis_code_12
    , nullif(diagnosis_code_13, '') as diagnosis_code_13
    , nullif(diagnosis_code_14, '') as diagnosis_code_14
    , nullif(diagnosis_code_15, '') as diagnosis_code_15
    , nullif(diagnosis_code_16, '') as diagnosis_code_16
    , nullif(diagnosis_code_17, '') as diagnosis_code_17
    , nullif(diagnosis_code_18, '') as diagnosis_code_18
    , nullif(diagnosis_code_19, '') as diagnosis_code_19
    , nullif(diagnosis_code_20, '') as diagnosis_code_20
    , nullif(diagnosis_code_21, '') as diagnosis_code_21
    , nullif(diagnosis_code_22, '') as diagnosis_code_22
    , nullif(diagnosis_code_23, '') as diagnosis_code_23
    , nullif(diagnosis_code_24, '') as diagnosis_code_24
    , nullif(diagnosis_code_25, '') as diagnosis_code_25
    , nullif(diagnosis_poa_1, '') as diagnosis_poa_1
    , nullif(diagnosis_poa_2, '') as diagnosis_poa_2
    , nullif(diagnosis_poa_3, '') as diagnosis_poa_3
    , nullif(diagnosis_poa_4, '') as diagnosis_poa_4
    , nullif(diagnosis_poa_5, '') as diagnosis_poa_5
    , nullif(diagnosis_poa_6, '') as diagnosis_poa_6
    , nullif(diagnosis_poa_7, '') as diagnosis_poa_7
    , nullif(diagnosis_poa_8, '') as diagnosis_poa_8
    , nullif(diagnosis_poa_9, '') as diagnosis_poa_9
    , nullif(diagnosis_poa_10, '') as diagnosis_poa_10
    , nullif(diagnosis_poa_11, '') as diagnosis_poa_11
    , nullif(diagnosis_poa_12, '') as diagnosis_poa_12
    , nullif(diagnosis_poa_13, '') as diagnosis_poa_13
    , nullif(diagnosis_poa_14, '') as diagnosis_poa_14
    , nullif(diagnosis_poa_15, '') as diagnosis_poa_15
    , nullif(diagnosis_poa_16, '') as diagnosis_poa_16
    , nullif(diagnosis_poa_17, '') as diagnosis_poa_17
    , nullif(diagnosis_poa_18, '') as diagnosis_poa_18
    , nullif(diagnosis_poa_19, '') as diagnosis_poa_19
    , nullif(diagnosis_poa_20, '') as diagnosis_poa_20
    , nullif(diagnosis_poa_21, '') as diagnosis_poa_21
    , nullif(diagnosis_poa_22, '') as diagnosis_poa_22
    , nullif(diagnosis_poa_23, '') as diagnosis_poa_23
    , nullif(diagnosis_poa_24, '') as diagnosis_poa_24
    , nullif(diagnosis_poa_25, '') as diagnosis_poa_25
    , nullif(procedure_code_type, '') as procedure_code_type
    , nullif(procedure_code_1, '') as procedure_code_1
    , nullif(procedure_code_2, '') as procedure_code_2
    , nullif(procedure_code_3, '') as procedure_code_3
    , nullif(procedure_code_4, '') as procedure_code_4
    , nullif(procedure_code_5, '') as procedure_code_5
    , nullif(procedure_code_6, '') as procedure_code_6
    , nullif(procedure_code_7, '') as procedure_code_7
    , nullif(procedure_code_8, '') as procedure_code_8
    , nullif(procedure_code_9, '') as procedure_code_9
    , nullif(procedure_code_10, '') as procedure_code_10
    , nullif(procedure_code_11, '') as procedure_code_11
    , nullif(procedure_code_12, '') as procedure_code_12
    , nullif(procedure_code_13, '') as procedure_code_13
    , nullif(procedure_code_14, '') as procedure_code_14
    , nullif(procedure_code_15, '') as procedure_code_15
    , nullif(procedure_code_16, '') as procedure_code_16
    , nullif(procedure_code_17, '') as procedure_code_17
    , nullif(procedure_code_18, '') as procedure_code_18
    , nullif(procedure_code_19, '') as procedure_code_19
    , nullif(procedure_code_20, '') as procedure_code_20
    , nullif(procedure_code_21, '') as procedure_code_21
    , nullif(procedure_code_22, '') as procedure_code_22
    , nullif(procedure_code_23, '') as procedure_code_23
    , nullif(procedure_code_24, '') as procedure_code_24
    , nullif(procedure_code_25, '') as procedure_code_25
    , procedure_date_1
    , procedure_date_2
    , procedure_date_3
    , procedure_date_4
    , procedure_date_5
    , procedure_date_6
    , procedure_date_7
    , procedure_date_8
    , procedure_date_9
    , procedure_date_10
    , procedure_date_11
    , procedure_date_12
    , procedure_date_13
    , procedure_date_14
    , procedure_date_15
    , procedure_date_16
    , procedure_date_17
    , procedure_date_18
    , procedure_date_19
    , procedure_date_20
    , procedure_date_21
    , procedure_date_22
    , procedure_date_23
    , procedure_date_24
    , procedure_date_25
    , nullif(data_source, '') as data_source
from {{ var('medical_claim')}}
