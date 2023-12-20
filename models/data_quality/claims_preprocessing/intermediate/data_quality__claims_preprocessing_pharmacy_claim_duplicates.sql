{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

select distinct
      test_catalog.source_table
    , 'all' as claim_type
    , 'claim_id' as grain
    , claim_id
    , data_source
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__pharmacy_claim') }}
     left join test_catalog
       on test_catalog.test_name = 'duplicate pharmacy claims'
       and test_catalog.source_table = 'normalized_input__pharmacy_claim'
group by
      claim_id
    , claim_line_number
    , data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
having count(*) > 1