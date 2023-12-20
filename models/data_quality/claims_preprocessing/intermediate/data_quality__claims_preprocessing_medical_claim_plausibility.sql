{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with medical_claim as (

    select *
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

, claim_start_date_after_claim_end_date as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'claim_start_date after claim_end_date'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_start_date > medical_claim.claim_end_date
    group by
          medical_claim.claim_id
        , medical_claim.data_source
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
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'admission_date after discharge_date'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and medical_claim.admission_date > medical_claim.discharge_date
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, admission_date_incorrect as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'admission_date incorrectly populated'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'professional'
    and medical_claim.admission_date is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, discharge_date_incorrect as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'discharge_date incorrectly populated'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'professional'
    and medical_claim.discharge_date is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, revenue_center_code_incorrect as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'revenue_center_code incorrectly populated'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'professional'
    and medical_claim.revenue_center_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, institutional_header_incorrect as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'institutional header-level fields incorrectly populated'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'professional'
    and (
        medical_claim.admit_type_code is not null
        or medical_claim.admit_type_code is not null
        or medical_claim.admit_source_code is not null
        or medical_claim.discharge_disposition_code is not null
        or medical_claim.bill_type_code is not null
        or medical_claim.ms_drg_code is not null
        or medical_claim.apr_drg_code is not null
    )
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, place_of_service_code_incorrect as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'professional' as claim_type
        , test_catalog.test_category
        , 'claim_id' as grain
        , medical_claim.claim_id
        , medical_claim.data_source
        , count(*) as counts
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from medical_claim
         left join test_catalog
           on test_catalog.test_name = 'place_of_service_code incorrectly populated'
           and test_catalog.source_table = 'normalized_input__medical_claim'
    where medical_claim.claim_type = 'institutional'
    and medical_claim.place_of_service_code is not null
    group by
          medical_claim.claim_id
        , medical_claim.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

select * from claim_start_date_after_claim_end_date
union all
select * from admission_date_after_discharge_date
union all
select * from admission_date_incorrect
union all
select * from discharge_date_incorrect
union all
select * from revenue_center_code_incorrect
union all
select * from institutional_header_incorrect
union all
select * from place_of_service_code_incorrect