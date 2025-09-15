with normalized_input__stg_medical_claim as (
    select *
    from {{ ref('the_tuva_project', 'normalized_input__stg_medical_claim') }}
)
select
    medical_claim_sk
    , data_source
    , claim_id
    , claim_line_number
    , claim_type
    , person_id
    , member_id
    , payer
    , {{ quote_column('plan') }}
    , min(claim_start_date) over(partition by data_source, claim_id) as claim_start_date
    , max(claim_end_date) over(partition by data_source, claim_id) as claim_end_date
    , claim_line_start_date
    , claim_line_end_date
    , min(admission_date) over(partition by data_source, claim_id) as admission_date
    , max(discharge_date) over(partition by data_source, claim_id) as discharge_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , place_of_service_code
    , bill_type_code
    , drg_code_type
    , drg_code
    , revenue_center_code
    , service_unit_quantity
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , rendering_npi
    , rendering_tin
    , billing_npi
    , billing_tin
    , facility_npi
    , paid_date
    , round(paid_amount, 2) as paid_amount
    , round(allowed_amount, 2) as allowed_amount
    , round(charge_amount, 2) as charge_amount
    , round(coinsurance_amount, 2) as coinsurance_amount
    , round(copayment_amount, 2) as copayment_amount
    , round(deductible_amount, 2) as deductible_amount
    , round(total_cost_amount, 2) as total_cost_amount
    , diagnosis_code_type
    , replace(diagnosis_code_1,'.','') as diagnosis_code_1
    , replace(diagnosis_code_2,'.','') as diagnosis_code_2
    , replace(diagnosis_code_3,'.','') as diagnosis_code_3
    , replace(diagnosis_code_4,'.','') as diagnosis_code_4
    , replace(diagnosis_code_5,'.','') as diagnosis_code_5
    , replace(diagnosis_code_6,'.','') as diagnosis_code_6
    , replace(diagnosis_code_7,'.','') as diagnosis_code_7
    , replace(diagnosis_code_8,'.','') as diagnosis_code_8
    , replace(diagnosis_code_9,'.','') as diagnosis_code_9
    , replace(diagnosis_code_10,'.','') as diagnosis_code_10
    , replace(diagnosis_code_11,'.','') as diagnosis_code_11
    , replace(diagnosis_code_12,'.','') as diagnosis_code_12
    , replace(diagnosis_code_13,'.','') as diagnosis_code_13
    , replace(diagnosis_code_14,'.','') as diagnosis_code_14
    , replace(diagnosis_code_15,'.','') as diagnosis_code_15
    , replace(diagnosis_code_16,'.','') as diagnosis_code_16
    , replace(diagnosis_code_17,'.','') as diagnosis_code_17
    , replace(diagnosis_code_18,'.','') as diagnosis_code_18
    , replace(diagnosis_code_19,'.','') as diagnosis_code_19
    , replace(diagnosis_code_20,'.','') as diagnosis_code_20
    , replace(diagnosis_code_21,'.','') as diagnosis_code_21
    , replace(diagnosis_code_22,'.','') as diagnosis_code_22
    , replace(diagnosis_code_23,'.','') as diagnosis_code_23
    , replace(diagnosis_code_24,'.','') as diagnosis_code_24
    , replace(diagnosis_code_25,'.','') as diagnosis_code_25
    , diagnosis_poa_1
    , diagnosis_poa_2
    , diagnosis_poa_3
    , diagnosis_poa_4
    , diagnosis_poa_5
    , diagnosis_poa_6
    , diagnosis_poa_7
    , diagnosis_poa_8
    , diagnosis_poa_9
    , diagnosis_poa_10
    , diagnosis_poa_11
    , diagnosis_poa_12
    , diagnosis_poa_13
    , diagnosis_poa_14
    , diagnosis_poa_15
    , diagnosis_poa_16
    , diagnosis_poa_17
    , diagnosis_poa_18
    , diagnosis_poa_19
    , diagnosis_poa_20
    , diagnosis_poa_21
    , diagnosis_poa_22
    , diagnosis_poa_23
    , diagnosis_poa_24
    , diagnosis_poa_25
    , procedure_code_type
    , replace(procedure_code_1,'.','') as procedure_code_1
    , replace(procedure_code_2,'.','') as procedure_code_2
    , replace(procedure_code_3,'.','') as procedure_code_3
    , replace(procedure_code_4,'.','') as procedure_code_4
    , replace(procedure_code_5,'.','') as procedure_code_5
    , replace(procedure_code_6,'.','') as procedure_code_6
    , replace(procedure_code_7,'.','') as procedure_code_7
    , replace(procedure_code_8,'.','') as procedure_code_8
    , replace(procedure_code_9,'.','') as procedure_code_9
    , replace(procedure_code_10,'.','') as procedure_code_10
    , replace(procedure_code_11,'.','') as procedure_code_11
    , replace(procedure_code_12,'.','') as procedure_code_12
    , replace(procedure_code_13,'.','') as procedure_code_13
    , replace(procedure_code_14,'.','') as procedure_code_14
    , replace(procedure_code_15,'.','') as procedure_code_15
    , replace(procedure_code_16,'.','') as procedure_code_16
    , replace(procedure_code_17,'.','') as procedure_code_17
    , replace(procedure_code_18,'.','') as procedure_code_18
    , replace(procedure_code_19,'.','') as procedure_code_19
    , replace(procedure_code_20,'.','') as procedure_code_20
    , replace(procedure_code_21,'.','') as procedure_code_21
    , replace(procedure_code_22,'.','') as procedure_code_22
    , replace(procedure_code_23,'.','') as procedure_code_23
    , replace(procedure_code_24,'.','') as procedure_code_24
    , replace(procedure_code_25,'.','') as procedure_code_25
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
    , in_network_flag
    , file_name
    , file_date
    , ingest_datetime
from normalized_input__stg_medical_claim