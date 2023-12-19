{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set pharmacy_header_column_list = [
      'claim_id'
    , 'patient_id'
    , 'member_id'
    , 'payer'
    , 'plan'
    , 'data_source'
] -%}

with pharmacy_header_duplicates as (

 {{ pharmacy_claim_header_duplicate_check(builtins.ref('normalized_input__pharmacy_claim'), pharmacy_header_column_list) }}

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
    , 'all' as claim_type
    , 'claim_id' as grain
    , pharmacy_header_duplicates.claim_id
    , pharmacy_header_duplicates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pharmacy_header_duplicates
     left join test_catalog
       on test_catalog.test_name = pharmacy_header_duplicates.column_checked||' non-unique'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by 
      pharmacy_header_duplicates.claim_id
    , pharmacy_header_duplicates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test