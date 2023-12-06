{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set professional_header_column_list = [
    'claim_id'
    , 'claim_type'
    , 'patient_id'
    , 'member_id'
    , 'claim_start_date'
    , 'claim_end_date'
    , 'place_of_service_code'
    , 'billing_npi'
    , 'paid_date'
    , 'diagnosis_code_type'
    , 'diagnosis_code_1'
    , 'diagnosis_code_2'
    , 'diagnosis_code_3'
    , 'diagnosis_code_4'
    , 'diagnosis_code_5'
    , 'diagnosis_code_6'
    , 'diagnosis_code_7'
    , 'diagnosis_code_8'
    , 'diagnosis_code_9'
    , 'diagnosis_code_10'
    , 'diagnosis_code_11'
    , 'diagnosis_code_12'
    , 'diagnosis_code_13'
    , 'diagnosis_code_14'
    , 'diagnosis_code_15'
    , 'diagnosis_code_16'
    , 'diagnosis_code_17'
    , 'diagnosis_code_18'
    , 'diagnosis_code_19'
    , 'diagnosis_code_20'
    , 'diagnosis_code_21'
    , 'diagnosis_code_22'
    , 'diagnosis_code_23'
    , 'diagnosis_code_24'
    , 'diagnosis_code_25'
    , 'data_source'
] -%}

with professional_header_duplicates as(

 {{ header_duplicate_check(builtins.ref('data_profiling__professional_header_failures'), professional_header_column_list, 'professional') }}

)

select
      'medical_claim' as source_table
    , 'professional' as claim_type
    , 'claim_id' as grain
    ,  claim_id
    , 'header' as test_category
    , column_checked||' duplicated' as test_name
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from professional_header_duplicates
group by 
    claim_id
    , column_checked||' duplicated'
    