{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set professional_missing_column_list = [
      'claim_id'
    , 'claim_line_number'
    , 'patient_id'
    , 'member_id'
    , 'payer'
    , 'plan'
    , 'claim_start_date'
    , 'claim_end_date'
    , 'place_of_service_code'
    , 'hcpcs_code'
    , 'rendering_npi'
    , 'billing_npi'
    , 'paid_date'
    , 'paid_amount'
    , 'diagnosis_code_type'
    , 'diagnosis_code_1'
    , 'data_source'
] -%}

with professional_missing as (

 {{ medical_claim_missing_column_check(builtins.ref('normalized_input__medical_claim'), professional_missing_column_list, 'professional') }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

select
      test_catalog.source_table
    , 'professional' as claim_type
    , 'claim_id' as grain
    , professional_missing.claim_id
    , professional_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from professional_missing
     left join test_catalog
       on test_catalog.test_name = professional_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      professional_missing.claim_id
    , professional_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test