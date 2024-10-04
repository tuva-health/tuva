{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with total_claims as(
    select
        count(claim_id) as total_claims
    from core.medical_claim
)
, claims_with_primary_dx as(
    select distinct
        claim_id
    from core.condition
    where condition_rank = 1

)

, claims_with_secondary_dx as(
    select
    total_claims
    , secondary_dx_claim_count
    , secondary_dx_claim_count/total_claims * 100 as percent_claim_with_secondar_dx
    , 'Percent of claims with secondary diagnosis' as data_quality_check
    from
    (
    select
        count(distinct claim_id) as secondary_dx_claim_count
    from core.condition
    where condition_rank >= 2
    )
    cross join total_claims
)
, missing_primary_dx as (
    select
        'missing primary diagnosis' as data_quality_check
        , count(*)
    from core.medical_claim m
    right join claims_with_primary_dx pdx
        on m.claim_id = pdx.claim_id
    where m.claim_id is null
)
, invalid_primary_dx as(
    select
        'invalid primary diagnosis' as data_quality_check
        , count(*)
    from core.condition
    where condition_rank = 1
    and normalized_code is null
)
, multiple_primary_dx as(

    select
        'multiple primary diagnosis' as data_quality_check
        , count(*)
    from
        (
            select
                claim_id
                , row_number() over (partition by claim_id order by claim_id) as row_nbr
            from core.condition
            where condition_rank = 1
            group by
                claim_id
            )
    where row_nbr > 1

)
, invalid_secondary_dx as(
    select
        'multiple primary diagnosis' as data_quality_check
        , count(*)
    from core.condition
    where condition_rank = 2
    and normalized_code is null
)
, invalid_procedure as(
    select
        'invalid procedure' as data_quality_check
        , count(*)
    from core.procedure
    where normalized_code is null
)

select * from missing_primary_dx
union all
select * from invalid_primary_dx
union all
select * from multiple_primary_dx
union all
select * from invalid_secondary_dx
union all
select data_quality_check, percent_claim_with_secondar_dx from claims_with_secondary_dx
union all
select * from invalid_procedure

