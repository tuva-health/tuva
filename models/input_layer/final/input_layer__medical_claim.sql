{{ config(
     enabled = var('input_layer_enabled',var('tuva_marts_enabled',True))
   )
}}

select
         cast(claim_id as {{ dbt.type_string() }} ) as claim_id
       , cast(claim_line_number as integer ) as claim_line_number
       , cast(claim_type as {{ dbt.type_string() }} ) as claim_type
       , cast(patient_id as {{ dbt.type_string() }} ) as patient_id
       , cast(member_id as {{ dbt.type_string() }} ) as member_id
       , cast(claim_start_date as date ) as claim_start_date
       , cast(claim_end_date as date ) as claim_end_date
       , cast(claim_line_start_date as date ) as claim_line_start_date
       , cast(claim_line_end_date as date ) as claim_line_end_date
       , cast(admission_date as date ) as admission_date
       , cast(discharge_date as date ) as discharge_date
       , cast(admit_source_code as {{ dbt.type_string() }} ) as admit_source_code
       , cast(admit_type_code as {{ dbt.type_string() }} ) as admit_type_code
       , cast(discharge_disposition_code as {{ dbt.type_string() }} ) as discharge_disposition_code
       , cast(place_of_service_code as {{ dbt.type_string() }} ) as place_of_service_code
       , cast(bill_type_code as {{ dbt.type_string() }} ) as bill_type_code
       , cast(ms_drg_code as {{ dbt.type_string() }} ) as ms_drg_code
       , cast(apr_drg_code as {{ dbt.type_string() }} ) as apr_drg_code
       , cast(revenue_center_code as {{ dbt.type_string() }} ) as revenue_center_code
       , cast(service_unit_quantity as integer ) as service_unit_quantity
       , cast(hcpcs_code as {{ dbt.type_string() }} ) as hcpcs_code
       , cast(hcpcs_modifier_1 as {{ dbt.type_string() }} ) as hcpcs_modifier_1
       , cast(hcpcs_modifier_2 as {{ dbt.type_string() }} ) as hcpcs_modifier_2
       , cast(hcpcs_modifier_3 as {{ dbt.type_string() }} ) as hcpcs_modifier_3
       , cast(hcpcs_modifier_4 as {{ dbt.type_string() }} ) as hcpcs_modifier_4
       , cast(hcpcs_modifier_5 as {{ dbt.type_string() }} ) as hcpcs_modifier_5
       , cast(rendering_npi as {{ dbt.type_string() }} ) as rendering_npi
       , cast(billing_npi as {{ dbt.type_string() }} ) as billing_npi
       , cast(facility_npi as {{ dbt.type_string() }} ) as facility_npi
       , cast(paid_date as date ) as paid_date
       , cast(paid_amount as {{ dbt.type_numeric() }} ) as paid_amount
       , cast(total_cost_amount as {{ dbt.type_numeric() }} ) as total_cost_amount
       , cast(allowed_amount as {{ dbt.type_numeric() }} ) as allowed_amount
       , cast(charge_amount as {{ dbt.type_numeric() }} ) as charge_amount
       , cast(diagnosis_code_type as {{ dbt.type_string() }} ) as diagnosis_code_type
       , cast(diagnosis_code_1 as {{ dbt.type_string() }} ) as diagnosis_code_1
       , cast(diagnosis_code_2 as {{ dbt.type_string() }} ) as diagnosis_code_2
       , cast(diagnosis_code_3 as {{ dbt.type_string() }} ) as diagnosis_code_3
       , cast(diagnosis_code_4 as {{ dbt.type_string() }} ) as diagnosis_code_4
       , cast(diagnosis_code_5 as {{ dbt.type_string() }} ) as diagnosis_code_5
       , cast(diagnosis_code_6 as {{ dbt.type_string() }} ) as diagnosis_code_6
       , cast(diagnosis_code_7 as {{ dbt.type_string() }} ) as diagnosis_code_7
       , cast(diagnosis_code_8 as {{ dbt.type_string() }} ) as diagnosis_code_8
       , cast(diagnosis_code_9 as {{ dbt.type_string() }} ) as diagnosis_code_9
       , cast(diagnosis_code_10 as {{ dbt.type_string() }} ) as diagnosis_code_10
       , cast(diagnosis_code_11 as {{ dbt.type_string() }} ) as diagnosis_code_11
       , cast(diagnosis_code_12 as {{ dbt.type_string() }} ) as diagnosis_code_12
       , cast(diagnosis_code_13 as {{ dbt.type_string() }} ) as diagnosis_code_13
       , cast(diagnosis_code_14 as {{ dbt.type_string() }} ) as diagnosis_code_14
       , cast(diagnosis_code_15 as {{ dbt.type_string() }} ) as diagnosis_code_15
       , cast(diagnosis_code_16 as {{ dbt.type_string() }} ) as diagnosis_code_16
       , cast(diagnosis_code_17 as {{ dbt.type_string() }} ) as diagnosis_code_17
       , cast(diagnosis_code_18 as {{ dbt.type_string() }} ) as diagnosis_code_18
       , cast(diagnosis_code_19 as {{ dbt.type_string() }} ) as diagnosis_code_19
       , cast(diagnosis_code_20 as {{ dbt.type_string() }} ) as diagnosis_code_20
       , cast(diagnosis_code_21 as {{ dbt.type_string() }} ) as diagnosis_code_21
       , cast(diagnosis_code_22 as {{ dbt.type_string() }} ) as diagnosis_code_22
       , cast(diagnosis_code_23 as {{ dbt.type_string() }} ) as diagnosis_code_23
       , cast(diagnosis_code_24 as {{ dbt.type_string() }} ) as diagnosis_code_24
       , cast(diagnosis_code_25 as {{ dbt.type_string() }} ) as diagnosis_code_25
       , cast(diagnosis_poa_1 as {{ dbt.type_string() }} ) as diagnosis_poa_1
       , cast(diagnosis_poa_2 as {{ dbt.type_string() }} ) as diagnosis_poa_2
       , cast(diagnosis_poa_3 as {{ dbt.type_string() }} ) as diagnosis_poa_3
       , cast(diagnosis_poa_4 as {{ dbt.type_string() }} ) as diagnosis_poa_4
       , cast(diagnosis_poa_5 as {{ dbt.type_string() }} ) as diagnosis_poa_5
       , cast(diagnosis_poa_6 as {{ dbt.type_string() }} ) as diagnosis_poa_6
       , cast(diagnosis_poa_7 as {{ dbt.type_string() }} ) as diagnosis_poa_7
       , cast(diagnosis_poa_8 as {{ dbt.type_string() }} ) as diagnosis_poa_8
       , cast(diagnosis_poa_9 as {{ dbt.type_string() }} ) as diagnosis_poa_9
       , cast(diagnosis_poa_10 as {{ dbt.type_string() }} ) as diagnosis_poa_10
       , cast(diagnosis_poa_11 as {{ dbt.type_string() }} ) as diagnosis_poa_11
       , cast(diagnosis_poa_12 as {{ dbt.type_string() }} ) as diagnosis_poa_12
       , cast(diagnosis_poa_13 as {{ dbt.type_string() }} ) as diagnosis_poa_13
       , cast(diagnosis_poa_14 as {{ dbt.type_string() }} ) as diagnosis_poa_14
       , cast(diagnosis_poa_15 as {{ dbt.type_string() }} ) as diagnosis_poa_15
       , cast(diagnosis_poa_16 as {{ dbt.type_string() }} ) as diagnosis_poa_16
       , cast(diagnosis_poa_17 as {{ dbt.type_string() }} ) as diagnosis_poa_17
       , cast(diagnosis_poa_18 as {{ dbt.type_string() }} ) as diagnosis_poa_18
       , cast(diagnosis_poa_19 as {{ dbt.type_string() }} ) as diagnosis_poa_19
       , cast(diagnosis_poa_20 as {{ dbt.type_string() }} ) as diagnosis_poa_20
       , cast(diagnosis_poa_21 as {{ dbt.type_string() }} ) as diagnosis_poa_21
       , cast(diagnosis_poa_22 as {{ dbt.type_string() }} ) as diagnosis_poa_22
       , cast(diagnosis_poa_23 as {{ dbt.type_string() }} ) as diagnosis_poa_23
       , cast(diagnosis_poa_24 as {{ dbt.type_string() }} ) as diagnosis_poa_24
       , cast(diagnosis_poa_25 as {{ dbt.type_string() }} ) as diagnosis_poa_25
       , cast(procedure_code_type as {{ dbt.type_string() }} ) as procedure_code_type
       , cast(procedure_code_1 as {{ dbt.type_string() }} ) as procedure_code_1
       , cast(procedure_code_2 as {{ dbt.type_string() }} ) as procedure_code_2
       , cast(procedure_code_3 as {{ dbt.type_string() }} ) as procedure_code_3
       , cast(procedure_code_4 as {{ dbt.type_string() }} ) as procedure_code_4
       , cast(procedure_code_5 as {{ dbt.type_string() }} ) as procedure_code_5
       , cast(procedure_code_6 as {{ dbt.type_string() }} ) as procedure_code_6
       , cast(procedure_code_7 as {{ dbt.type_string() }} ) as procedure_code_7
       , cast(procedure_code_8 as {{ dbt.type_string() }} ) as procedure_code_8
       , cast(procedure_code_9 as {{ dbt.type_string() }} ) as procedure_code_9
       , cast(procedure_code_10 as {{ dbt.type_string() }} ) as procedure_code_10
       , cast(procedure_code_11 as {{ dbt.type_string() }} ) as procedure_code_11
       , cast(procedure_code_12 as {{ dbt.type_string() }} ) as procedure_code_12
       , cast(procedure_code_13 as {{ dbt.type_string() }} ) as procedure_code_13
       , cast(procedure_code_14 as {{ dbt.type_string() }} ) as procedure_code_14
       , cast(procedure_code_15 as {{ dbt.type_string() }} ) as procedure_code_15
       , cast(procedure_code_16 as {{ dbt.type_string() }} ) as procedure_code_16
       , cast(procedure_code_17 as {{ dbt.type_string() }} ) as procedure_code_17
       , cast(procedure_code_18 as {{ dbt.type_string() }} ) as procedure_code_18
       , cast(procedure_code_19 as {{ dbt.type_string() }} ) as procedure_code_19
       , cast(procedure_code_20 as {{ dbt.type_string() }} ) as procedure_code_20
       , cast(procedure_code_21 as {{ dbt.type_string() }} ) as procedure_code_21
       , cast(procedure_code_22 as {{ dbt.type_string() }} ) as procedure_code_22
       , cast(procedure_code_23 as {{ dbt.type_string() }} ) as procedure_code_23
       , cast(procedure_code_24 as {{ dbt.type_string() }} ) as procedure_code_24
       , cast(procedure_code_25 as {{ dbt.type_string() }} ) as procedure_code_25
       , cast(procedure_date_1 as date ) as procedure_date_1
       , cast(procedure_date_2 as date ) as procedure_date_2
       , cast(procedure_date_3 as date ) as procedure_date_3
       , cast(procedure_date_4 as date ) as procedure_date_4
       , cast(procedure_date_5 as date ) as procedure_date_5
       , cast(procedure_date_6 as date ) as procedure_date_6
       , cast(procedure_date_7 as date ) as procedure_date_7
       , cast(procedure_date_8 as date ) as procedure_date_8
       , cast(procedure_date_9 as date ) as procedure_date_9
       , cast(procedure_date_10 as date ) as procedure_date_10
       , cast(procedure_date_11 as date ) as procedure_date_11
       , cast(procedure_date_12 as date ) as procedure_date_12
       , cast(procedure_date_13 as date ) as procedure_date_13
       , cast(procedure_date_14 as date ) as procedure_date_14
       , cast(procedure_date_15 as date ) as procedure_date_15
       , cast(procedure_date_16 as date ) as procedure_date_16
       , cast(procedure_date_17 as date ) as procedure_date_17
       , cast(procedure_date_18 as date ) as procedure_date_18
       , cast(procedure_date_19 as date ) as procedure_date_19
       , cast(procedure_date_20 as date ) as procedure_date_20
       , cast(procedure_date_21 as date ) as procedure_date_21
       , cast(procedure_date_22 as date ) as procedure_date_22
       , cast(procedure_date_23 as date ) as procedure_date_23
       , cast(procedure_date_24 as date ) as procedure_date_24
       , cast(procedure_date_25 as date ) as procedure_date_25
       , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('medical_claim')}}


