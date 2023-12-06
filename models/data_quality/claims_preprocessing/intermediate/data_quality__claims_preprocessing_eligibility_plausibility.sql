{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with multiple_genders_test as (
    select
        'multiple genders' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'plausibility' as test_category
        , 'patient_id' as grain
        , patient_id
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }}
    group by
        patient_id
    having count(distinct gender) > 1
)
, multiple_races_test as(
    select
        'multiple races' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'plausibility' as test_category
        , 'patient_id' as grain
        , patient_id
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} 
    group by
        patient_id
    having count(distinct race) > 1
)
, multiple_birth_dates_test as(
    select
        'multiple birth dates' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'plausibility' as test_category
        , 'patient_id' as grain
        , patient_id
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }}
    group by
        patient_id
    having count(distinct birth_date) > 1
)
, multiple_death_dates_test as(
    select
        'multiple death dates' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'plausibility' as test_category
        , 'patient_id' as grain
        , patient_id
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }}
    group by
        patient_id
    having count(distinct death_date) > 1
)
, birth_date_after_death_date as(
    select
        'birth date after death date' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'plausibility' as test_category
        , 'patient_id' as grain
        , patient_id
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }}
    where birth_date > death_date
    group by
        patient_id
    having count(distinct gender) > 1
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