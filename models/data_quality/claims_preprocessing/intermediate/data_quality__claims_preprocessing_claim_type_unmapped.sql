{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with medical_claim as (

    select
          claim_id
        , data_source
        , claim_type
    from {{ ref('normalized_input__medical_claim') }}

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
    , medical_claim.claim_id
    , medical_claim.data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from medical_claim
     left join test_catalog
       on test_catalog.test_name = 'claim_type missing'
       and test_catalog.source_table = 'normalized_input__medical_claim'
where medical_claim.claim_type is null
group by
      medical_claim.claim_id
    , medical_claim.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test