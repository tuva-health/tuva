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

, calendar as (

    select full_date
    from {{ ref('terminology__calendar') }}

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

, valid_claim_start_date as (

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
         left join calendar
           on medical_claim.claim_start_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'claim start date invalid'
           and test_catalog.source_table = 'medical_claim'
    where calendar.full_date is null
    and medical_claim.claim_start_date is not null
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_claim_end_date as (

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
         left join calendar
           on medical_claim.claim_end_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'claim end date invalid'
           and test_catalog.source_table = 'medical_claim'
    where calendar.full_date is null
    and medical_claim.claim_end_date is not null
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_claim_line_start_date as (

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
         left join calendar
           on medical_claim.claim_line_start_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'claim line start date invalid'
           and test_catalog.source_table = 'medical_claim'
    where calendar.full_date is null
    and medical_claim.claim_line_start_date is not null
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_claim_line_end_date as (

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
         left join calendar
           on medical_claim.claim_line_end_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'claim line end date invalid'
           and test_catalog.source_table = 'medical_claim'
    where calendar.full_date is null
    and medical_claim.claim_line_end_date is not null
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_admission_date as (

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
         left join calendar
           on medical_claim.admission_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'admission date invalid'
           and test_catalog.source_table = 'medical_claim'
    where medical_claim.claim_type = 'institutional'
    and calendar.full_date is null
    and medical_claim.admission_date is not null
    group by
          medical_claim.claim_id
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

, valid_discharge_date as (

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
         left join calendar
           on medical_claim.discharge_date = calendar.full_date
         left join test_catalog
           on test_catalog.test_name = 'discharge date invalid'
           and test_catalog.source_table = 'medical_claim'
    where medical_claim.claim_type = 'institutional'
    and calendar.full_date is null
    and medical_claim.discharge_date is not null
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
union all
select * from valid_claim_start_date
union all
select * from valid_claim_end_date
union all
select * from valid_claim_line_start_date
union all
select * from valid_claim_line_end_date
union all
select * from valid_admission_date
union all
select * from valid_discharge_date