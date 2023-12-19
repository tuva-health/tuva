{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set pharmacy_claim_missing_column_list = [
      'claim_id'
    , 'claim_line_number'
    , 'patient_id'
    , 'member_id'
    , 'payer'
    , 'plan'
    , 'prescribing_provider_npi'
    , 'dispensing_provider_npi'
    , 'dispensing_date'
    , 'ndc_code'
    , 'quantity'
    , 'days_supply'
    , 'refills'
    , 'paid_date'
    , 'paid_amount'
    , 'allowed_amount'
    , 'data_source'
] -%}

with pharmacy_claim_missing as (

 {{ pharmacy_claim_missing_column_check(builtins.ref('normalized_input__pharmacy_claim'), pharmacy_claim_missing_column_list) }}

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
    , 'all' as claim_type
    , 'claim_id' as grain
    , pharmacy_claim_missing.claim_id
    , pharmacy_claim_missing.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from pharmacy_claim_missing
     left join test_catalog
       on test_catalog.test_name = pharmacy_claim_missing.column_checked||' missing'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by
      pharmacy_claim_missing.claim_id
    , pharmacy_claim_missing.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test