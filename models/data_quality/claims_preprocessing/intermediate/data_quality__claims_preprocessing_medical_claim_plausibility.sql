{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with medical_claim as (

    select *
    from {{ ref('medical_claim') }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

, claim_start_date_after_claim_end_date as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'claim start date after claim end date'
           and test_catalog.source_table = 'medical_claim'
    where medical_claim.claim_start_date > medical_claim.claim_end_date
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, admission_date_after_discharge_date as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'institutional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'admission date after discharge date'
           and test_catalog.source_table = 'medical_claim'
    where medical_claim.claim_type = 'institutional'
    and medical_claim.admission_date > medical_claim.discharge_date
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

select * from claim_start_date_after_claim_end_date
union all
select * from admission_date_after_discharge_date