{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with eligiblity as (

    select *
    from {{ ref('normalized_input__eligibility') }}

)

, test_catalog as (

    select
          source_table
        , test_category
        , test_name
        , pipeline_test
    from {{ ref('data_quality__test_catalog') }}

)

, multiple_genders_test as (

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from eligiblity
         left join test_catalog
           on test_catalog.test_name = 'multiple genders'
           and test_catalog.source_table = 'normalized_input__eligibility'
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test
    having count(distinct eligiblity.gender) > 1

)

, multiple_races_test as(

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from eligiblity
         left join test_catalog
           on test_catalog.test_name = 'multiple races'
           and test_catalog.source_table = 'normalized_input__eligibility'
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test
    having count(distinct eligiblity.race) > 1

)

, multiple_birth_dates_test as(

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from eligiblity
         left join test_catalog
           on test_catalog.test_name = 'multiple birth dates'
           and test_catalog.source_table = 'normalized_input__eligibility'
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test
    having count(distinct eligiblity.birth_date) > 1

)

, multiple_death_dates_test as(

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from eligiblity
         left join test_catalog
           on test_catalog.test_name = 'multiple death dates'
           and test_catalog.source_table = 'normalized_input__eligibility'
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test
    having count(distinct eligiblity.death_date) > 1

)

, birth_date_after_death_date as(

    select
          test_catalog.test_name
        , test_catalog.pipeline_test
        , test_catalog.source_table
        , 'all' as claim_type
        , test_catalog.test_category
        , 'patient_id' as grain
        , eligiblity.patient_id
        , eligiblity.data_source
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from eligiblity
         left join test_catalog
           on test_catalog.test_name = 'birth date after death date'
           and test_catalog.source_table = 'normalized_input__eligibility'
    where eligiblity.birth_date > eligiblity.death_date
    group by
          eligiblity.patient_id
        , eligiblity.data_source
        , test_catalog.source_table
        , test_catalog.test_category
        , test_catalog.test_name
        , test_catalog.pipeline_test

)

select * from multiple_genders_test
union all
select * from multiple_races_test
union all
select * from multiple_birth_dates_test
union all
select * from multiple_death_dates_test
union all
select * from birth_date_after_death_date