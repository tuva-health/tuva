{{ config(
    enabled = var('claims_enabled', False)
) }}

with eligibility as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
)

, missing_person_id_count as (
    select
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where person_id is null
)

, missing_person_id_perc as (
    select
        round(
            (select eligibility_count from missing_person_id_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_person_id_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct person_id) as count_of_person_ids
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct person_id) > 1
    ) as subquery
)

, dupe_person_id_perc as (
    select 
        round(
            (select eligibility_count from dupe_person_id_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_gender_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where gender is null

)

, missing_gender_perc as (
    select 
        round(
            (select eligibility_count from missing_gender_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, invalid_gender_count as (
    select cast(count(distinct person_id) as {{ dbt.type_numeric()}}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__gender') }} bb
        on aa.gender = bb.gender
    where aa.gender is not null and bb.gender is null
    
)

, invalid_gender_perc as (
    select 
        round(
            (select eligibility_count from invalid_gender_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)
 
, dupe_gender_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct gender) as count_of_gender
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct gender) > 1
    ) as subquery
)

, dupe_gender_perc as (
    select 
        round(
            (select eligibility_count from dupe_gender_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, missing_race_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where race is null
)

, missing_race_perc as (
    select 
        round(
            (select eligibility_count from missing_race_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_race_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__race') }} bb
        on aa.race = bb.description
    where aa.race is not null and bb.description is null

)

, invalid_race_perc as (
    select
        round(
            ( select eligibility_count from invalid_race_count) * 100.0 /
            ( select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_race_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct race) as count_of_race
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct race) > 1
    ) as subquery

)

, dupe_race_perc as (
    select
    round(
        (select eligibility_count from dupe_race_count) * 100 /
        (select eligibility_count from eligibility), 1
    ) as percentage

)

, missing_birth_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where birth_date is null
)

, missing_birth_date_perc as (
    select 
        round(
            (select eligibility_count from missing_birth_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_birth_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.birth_date = bb.full_date
    where (aa.birth_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date) ) or (aa.birth_date is not null and bb.full_date is null) 

)

, invalid_birth_date_perc as (
    select
        round (
            (select eligibility_count from invalid_birth_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_birth_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct birth_date) as count_of_birth_dates
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct birth_date) > 1
    ) as subquery

)

, dupe_birth_date_perc as (
    select 
        round(
            (select eligibility_count from dupe_birth_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, missing_enrollment_start_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where enrollment_start_date is null

)

, missing_enrollment_start_date_perc as (
    select 
        round(
            (select eligibility_count from missing_enrollment_start_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, invalid_enrollment_start_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.enrollment_start_date = bb.full_date
    where (aa.enrollment_start_date is not null and bb.full_date is null) or (aa.enrollment_start_date > cast(substring('{{ var('tuva_last_run') }}',1,10) as date))

)

, invalid_enrollment_start_date_perc as (
    select 
        round(
            (select eligibility_count from invalid_enrollment_start_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_enrollment_start_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct enrollment_start_date) as count_of_enrollment_start_dates
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct enrollment_start_date) > 1
    ) as subquery

)

, dupe_enrollment_start_date_perc as (
    select 
        round(
            (select eligibility_count from dupe_enrollment_start_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, missing_enrollment_end_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where enrollment_end_date is null

)

, missing_enrollment_end_date_perc as (
    select 
        round(
            (select eligibility_count from missing_enrollment_end_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, invalid_enrollment_end_date_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('reference_data__calendar') }} bb
        on aa.enrollment_end_date = bb.full_date
    where (aa.enrollment_end_date is not null and bb.full_date is null) or (aa.enrollment_end_date < aa.enrollment_start_date)

)

, invalid_enrollment_end_date_perc as (
    select 
        round(
            (select eligibility_count from invalid_enrollment_end_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_enrollment_end_date_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct enrollment_end_date) as count_of_enrollment_end_dates
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct enrollment_end_date) > 1
    ) as subquery

)

, dupe_enrollment_end_date_perc as (
    select 
        round(
            (select eligibility_count from dupe_enrollment_end_date_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, missing_payer_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where payer is null

)

, missing_payer_perc as (
    select 
        round(
            (select eligibility_count from missing_payer_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_payer_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct payer) as count_of_payers
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct payer) > 1
    ) as subquery

)

, dupe_payer_perc as (
    select 
        round(
            (select eligibility_count from dupe_payer_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, missing_payer_type_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where payer_type is null
)

, missing_payer_type_perc as (
    select 
        round(
            (select eligibility_count from missing_payer_type_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_payer_type_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__payer_type') }} bb
        on aa.payer_type = bb.payer_type
    where aa.payer_type is not null and bb.payer_type is null

)

, invalid_payer_type_perc as (
    select 
        round(
            (select eligibility_count from invalid_payer_type_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, dupe_payer_type_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct payer_type) as count_of_payer_types
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct payer_type) > 1
    ) as subquery

)

, dupe_payer_type_perc as (
    select 
        round(
            (select eligibility_count from dupe_payer_type_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage

)

, missing_original_reason_entitlement_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }}
    where original_reason_entitlement_code is null

)

, missing_original_reason_entitlement_code_perc as (
    select 
        round(
            (select eligibility_count from missing_original_reason_entitlement_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, invalid_original_reason_entitlement_code_count as (
    select 
        cast(count(distinct person_id) as {{ dbt.type_numeric() }}) as eligibility_count
    from {{ ref('eligibility') }} aa
    left join {{ ref('terminology__medicare_orec')}} bb
        on aa.original_reason_entitlement_code = bb.original_reason_entitlement_code
    where aa.original_reason_entitlement_code is not null and bb.original_reason_entitlement_code is null

)

, invalid_original_reason_entitlement_code_perc as (
    select 
        round(
            (select eligibility_count from invalid_original_reason_entitlement_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
)

, dupe_original_reason_entitlement_code_count as (
    select 
        cast(count(*) as {{ dbt.type_numeric() }}) as eligibility_count
    from (
        select
            person_id,
            count(distinct original_reason_entitlement_code) as count_of_original_reason_entitlement_codes
        from {{ ref('eligibility') }}
        group by person_id
        having count(distinct original_reason_entitlement_code) > 1
    ) as subquery

)

,dupe_original_reason_entitlement_code_perc as (
    select 
        round(
            (select eligibility_count from dupe_original_reason_entitlement_code_count) * 100.0 /
            (select eligibility_count from eligibility), 1
        ) as percentage
) 


, final as (

    select
          'person_id' as field
        , 'all' as claim_type
        , (select * from missing_person_id_count) as missing_count
        , (select * from missing_person_id_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_person_id_count) as duplicated_count
        , (select * from dupe_person_id_perc) as duplicated_perc

    union all

    select
          'gender' as field
        , 'all' as claim_type
        , (select * from missing_gender_count) as missing_count
        , (select * from missing_gender_perc) as missing_perc
        , (select * from invalid_gender_count) as invalid_count
        , (select * from invalid_gender_perc) as invalid_perc
        , (select * from dupe_gender_count) as duplicated_count
        , (select * from dupe_gender_perc) as duplicated_perc

    union all

    select
          'race' as field
        , 'all' as claim_type
        , (select * from missing_race_count) as missing_count
        , (select * from missing_race_perc) as missing_perc
        , (select * from invalid_race_count) as invalid_count
        , (select * from invalid_race_perc) as invalid_perc
        , (select * from dupe_race_count) as duplicated_count
        , (select * from dupe_race_perc) as duplicated_perc
    
    union all

    select
          'birth_date' as field
        , 'all' as claim_type
        , (select * from missing_birth_date_count) as missing_count
        , (select * from missing_birth_date_perc) as missing_perc
        , (select * from invalid_birth_date_count) as invalid_count
        , (select * from invalid_birth_date_perc) as invalid_perc
        , (select * from dupe_birth_date_count) as duplicated_count
        , (select * from dupe_birth_date_perc) as duplicated_perc

    union all

    select
          'enrollment_start_date' as field
        , 'all' as claim_type
        , (select * from missing_enrollment_start_date_count) as missing_count
        , (select * from missing_enrollment_start_date_perc) as missing_perc
        , (select * from invalid_enrollment_start_date_count) as invalid_count
        , (select * from invalid_enrollment_start_date_perc) as invalid_perc
        , (select * from dupe_enrollment_start_date_count) as duplicated_count
        , (select * from dupe_enrollment_start_date_perc) as duplicated_perc

    union all

    select
          'enrollment_end_date' as field
        , 'all' as claim_type
        , (select * from missing_enrollment_end_date_count) as missing_count
        , (select * from missing_enrollment_end_date_perc) as missing_perc
        , (select * from invalid_enrollment_end_date_count) as invalid_count
        , (select * from invalid_enrollment_end_date_perc) as invalid_perc
        , (select * from dupe_enrollment_end_date_count) as duplicated_count
        , (select * from dupe_enrollment_end_date_perc) as duplicated_perc

    union all

    select
          'payer' as field
        , 'all' as claim_type
        , (select * from missing_payer_count) as missing_count
        , (select * from missing_payer_perc) as missing_perc
        , null as invalid_count
        , null as invalid_perc
        , (select * from dupe_payer_count) as duplicated_count
        , (select * from dupe_payer_perc) as duplicated_perc

    union all

    select
          'payer_type' as field
        , 'all' as claim_type
        , (select * from missing_payer_type_count) as missing_count
        , (select * from missing_payer_type_perc) as missing_perc
        , (select * from invalid_payer_type_count) as invalid_count
        , (select * from invalid_payer_type_perc) as invalid_perc
        , (select * from dupe_payer_type_count) as duplicated_count
        , (select * from dupe_payer_type_perc) as duplicated_perc

    union all

    select 
          'original_reason_entitlement_code' as field
        , 'all' as claim_type
        , (select * from missing_original_reason_entitlement_code_count) as missing_count
        , (select * from missing_original_reason_entitlement_code_perc) as missing_perc
        , (select * from invalid_original_reason_entitlement_code_count) as invalid_count
        , (select * from invalid_original_reason_entitlement_code_perc) as invalid_perc
        , (select * from dupe_original_reason_entitlement_code_count) as duplicated_count
        , (select * from dupe_original_reason_entitlement_code_perc) as duplicated_perc

)

select
      'eligibility' as table_name
    , field
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
    , claim_type
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
