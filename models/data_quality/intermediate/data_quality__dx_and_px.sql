{{ config(
     enabled = var('data_quality_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with total_claims as(
    select
        cast(count(claim_id) as {{ dbt.type_int() }} ) as total_claims
    from {{ ref('core__medical_claim') }}
)
, claims_with_primary_dx as(
    select distinct
        claim_id
    from {{ ref('core__condition') }}
    where condition_rank = 1

)

, claims_with_secondary_dx as(
    select
    total_claims
    , cast(secondary_dx_claim_count as {{ dbt.type_int() }} ) as secondary_dx_claim_count
    , (cast(secondary_dx_claim_count as {{ dbt.type_int() }} ) / cast(total_claims as {{ dbt.type_int() }} )) * 100 as percent_claim_with_secondar_dx
    , 'Percent of claims with secondary diagnosis' as data_quality_check
    from
    (
    select
        tc.total_claims
        , cast(count(distinct claim_id) as {{ dbt.type_int() }} ) as secondary_dx_claim_count
    from {{ ref('core__condition') }}
    cross join total_claims tc
    where condition_rank >= 2
    group by total_claims
    ) x
)
, missing_primary_dx as (
    select
        'missing primary diagnosis' as data_quality_check
        , cast(count(*) as {{ dbt.type_int() }} ) as missing_primary_dx_count
    from {{ ref('core__medical_claim') }} m
    right join claims_with_primary_dx pdx
        on m.claim_id = pdx.claim_id
    where m.claim_id is null
)
, invalid_primary_dx as(
    select
        'invalid primary diagnosis' as data_quality_check
        , cast(count(*) as {{ dbt.type_int() }} ) as invalid_primary_dx_count
    from {{ ref('core__condition') }}
    where condition_rank = 1
    and normalized_code is null
)
, multiple_primary_dx as(

    select
        'multiple primary diagnosis' as data_quality_check
        , cast(count(*) as {{ dbt.type_int() }} ) as multiple_primary_dx_count
    from
        (
            select
                claim_id
                , row_number() over (partition by claim_id order by claim_id) as row_nbr
            from {{ ref('core__condition') }}
            where condition_rank = 1
            group by
                claim_id
            )
    where row_nbr > 1

)
, invalid_secondary_dx as(
    select
        'multiple primary diagnosis' as data_quality_check
        , cast(count(*) as {{ dbt.type_int() }} ) as invalid_secondary_dx_count
    from {{ ref('core__condition') }}
    where condition_rank = 2
    and normalized_code is null
)
, invalid_procedure as(
    select
        'invalid procedure' as data_quality_check
        , cast(count(*) as {{ dbt.type_int() }} ) as invalid_px_count
    from {{ ref('core__procedure') }}
    where normalized_code is null
)

select *, '{{ var('tuva_last_run')}}' as tuva_last_run from missing_primary_dx
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_primary_dx
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from multiple_primary_dx
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_secondary_dx
union all
select data_quality_check, percent_claim_with_secondar_dx, '{{ var('tuva_last_run')}}' as tuva_last_run from claims_with_secondary_dx
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_procedure
