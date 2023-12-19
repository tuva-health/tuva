{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set institutional_header_column_list = [
      'claim_id'
    , 'claim_type'
    , 'patient_id'
    , 'member_id'
    , 'payer'
    , 'plan'
    , 'claim_start_date'
    , 'claim_end_date'
    , 'admission_date'
    , 'discharge_date'
    , 'admit_source_code'
    , 'admit_type_code'
    , 'discharge_disposition_code'
    , 'bill_type_code'
    , 'ms_drg_code'
    , 'facility_npi'
    , 'billing_npi'
    , 'rendering_npi'
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
    , 'diagnosis_poa_1'
    , 'diagnosis_poa_2'
    , 'diagnosis_poa_3'
    , 'diagnosis_poa_4'
    , 'diagnosis_poa_5'
    , 'diagnosis_poa_6'
    , 'diagnosis_poa_7'
    , 'diagnosis_poa_8'
    , 'diagnosis_poa_9'
    , 'diagnosis_poa_10'
    , 'diagnosis_poa_11'
    , 'diagnosis_poa_12'
    , 'diagnosis_poa_13'
    , 'diagnosis_poa_14'
    , 'diagnosis_poa_15'
    , 'diagnosis_poa_16'
    , 'diagnosis_poa_17'
    , 'diagnosis_poa_18'
    , 'diagnosis_poa_19'
    , 'diagnosis_poa_20'
    , 'diagnosis_poa_21'
    , 'diagnosis_poa_22'
    , 'diagnosis_poa_23'
    , 'diagnosis_poa_24'
    , 'diagnosis_poa_25'
    , 'procedure_code_type'
    , 'procedure_code_1'
    , 'procedure_code_2'
    , 'procedure_code_3'
    , 'procedure_code_4'
    , 'procedure_code_5'
    , 'procedure_code_6'
    , 'procedure_code_7'
    , 'procedure_code_8'
    , 'procedure_code_9'
    , 'procedure_code_10'
    , 'procedure_code_11'
    , 'procedure_code_12'
    , 'procedure_code_13'
    , 'procedure_code_14'
    , 'procedure_code_15'
    , 'procedure_code_16'
    , 'procedure_code_17'
    , 'procedure_code_18'
    , 'procedure_code_19'
    , 'procedure_code_20'
    , 'procedure_code_21'
    , 'procedure_code_22'
    , 'procedure_code_23'
    , 'procedure_code_24'
    , 'procedure_code_25'
    , 'procedure_date_1'
    , 'procedure_date_2'
    , 'procedure_date_3'
    , 'procedure_date_4'
    , 'procedure_date_5'
    , 'procedure_date_6'
    , 'procedure_date_7'
    , 'procedure_date_8'
    , 'procedure_date_9'
    , 'procedure_date_10'
    , 'procedure_date_11'
    , 'procedure_date_12'
    , 'procedure_date_13'
    , 'procedure_date_14'
    , 'procedure_date_15'
    , 'procedure_date_16'
    , 'procedure_date_17'
    , 'procedure_date_18'
    , 'procedure_date_19'
    , 'procedure_date_20'
    , 'procedure_date_21'
    , 'procedure_date_22'
    , 'procedure_date_23'
    , 'procedure_date_24'
    , 'procedure_date_25'
    , 'data_source'
] -%}


with institutional_header_duplicates as (

 {{ medical_claim_header_duplicate_check(builtins.ref('normalized_input__medical_claim'), institutional_header_column_list, 'institutional') }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
        , claim_type
    from {{ ref('data_quality__test_catalog') }}

)

select
      test_catalog.source_table
    , 'institutional' as claim_type
    , 'claim_id' as grain
    , institutional_header_duplicates.claim_id
    , institutional_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from institutional_header_duplicates
     left join test_catalog
       on test_catalog.test_name = institutional_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__medical_claim'
       and test_catalog.claim_type = 'institutional'
group by 
      institutional_header_duplicates.claim_id
    , institutional_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test