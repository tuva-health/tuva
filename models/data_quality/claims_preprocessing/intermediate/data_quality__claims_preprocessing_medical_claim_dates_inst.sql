{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

{% set claim_date_column_list = [
      'admission_date'
    , 'discharge_date'
] -%}

with claim_dates as (

 {{ medical_claim_date_check(builtins.ref('normalized_input__medical_claim'), claim_date_column_list, 'institutional') }}

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
    , 'institutional' as claim_type
    , 'claim_id' as grain
    , claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from claim_dates
     left join test_catalog
       on test_catalog.test_name = claim_dates.column_checked||' invalid'
       and test_catalog.source_table = 'normalized_input__medical_claim'
group by
      claim_dates.claim_id
    , claim_dates.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test