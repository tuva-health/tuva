{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with valid_gender as(
    select
        'gender invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.gender
        , count(elig.gender) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__gender') }} gender
        on elig.gender = gender.gender
    where gender.gender is null
    and elig.gender is not null
    group by
        patient_id
        , elig.gender
)
, valid_race as(
    select
        'race invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.race
        , count(elig.race) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__race') }} race
        on elig.race = race.description
    where race.description is null
    and elig.race is not null
    group by
        patient_id
        , elig.race
)
, valid_payer_type as(
    select
        'payer_type invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.payer_type
        , count(elig.payer_type) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__payer_type') }} payer
        on elig.payer_type = payer.payer_type
    where payer.payer_type is null
    and elig.payer_type is not null
    group by
        patient_id
        , elig.payer_type
)
, valid_orec as(
    select
        'orec invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.original_reason_entitlement_code
        , count(elig.original_reason_entitlement_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__medicare_orec') }} orec
        on elig.original_reason_entitlement_code = orec.original_reason_entitlement_code
    where orec.original_reason_entitlement_code is null
    and elig.original_reason_entitlement_code is not null
    group by
        patient_id
        , elig.original_reason_entitlement_code
)
, valid_dual_status_code as(
    select
        'dual_status_code invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.dual_status_code
        , count(elig.dual_status_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__medicare_dual_eligibility') }} dual
        on elig.dual_status_code = dual.dual_status_code
    where dual.dual_status_code is null
    and elig.dual_status_code is not null
    group by
        patient_id
        , elig.dual_status_code
)
, valid_medicare_status_code as(
    select
        'medicare_status_code invalid' as test_name
        , 'eligibility' as source_table
        , 'all' as claim_type
        , 'invalid_values' as test_category
        , 'patient_id' as grain
        , patient_id
        , elig.medicare_status_code
        , count(elig.medicare_status_code) as filled_row_count
        , '{{ var('tuva_last_run')}}' as tuva_last_run
    from {{ ref('eligibility') }} elig
    left join {{ ref('terminology__medicare_status') }} status
        on elig.medicare_status_code = status.medicare_status_code
    where status.medicare_status_code is null
    and elig.medicare_status_code is not null
    group by
        patient_id
        , elig.medicare_status_code
)
select * from valid_gender
union all
select * from valid_race
union all
select * from valid_payer_type
union all
select * from valid_orec
union all
select * from valid_dual_status_code
union all
select * from valid_medicare_status_code
