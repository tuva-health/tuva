{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set professional_header_column_list = [
      'claim_id'
    , 'claim_type'
    , 'patient_id'
    , 'member_id'
    , 'payer'
    , 'plan'
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

with professional_header_duplicates as (

 {{ medical_claim_header_duplicate_check(builtins.ref('normalized_input__medical_claim'), professional_header_column_list, 'professional') }}

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
    , 'professional' as claim_type
    , 'claim_id' as grain
    , professional_header_duplicates.claim_id
    , professional_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from professional_header_duplicates
     left join test_catalog
       on test_catalog.test_name = professional_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__medical_claim'
       and test_catalog.claim_type = 'professional'
group by 
      professional_header_duplicates.claim_id
    , professional_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test