{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with eligibility as (

    select
          patient_id
        , enrollment_start_date
        , enrollment_end_date
        , payer
        , plan
        , data_source
    from {{ ref('eligibility') }}

)

, test_catalog as (

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
    , 'patient_id' as grain
    ,  patient_id
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from eligibility
     left join test_catalog
       on test_catalog.test_name = 'duplicate eligibility'
       and test_catalog.source_table = 'eligibility'
group by
      eligibility.patient_id
    , eligibility.enrollment_start_date
    , eligibility.enrollment_end_date
    , eligibility.payer
    , eligibility.plan
    , eligibility.data_source
    , test_catalog.source_table
    , test_catalog.test_category
    , test_catalog.test_name
    , test_catalog.pipeline_test
having count(*) > 1