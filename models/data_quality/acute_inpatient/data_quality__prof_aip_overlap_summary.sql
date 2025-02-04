{{ config(
    enabled = var('claims_enabled', False)
) }}

with number_of_encounters_each_prof_claim_overlaps_with as (

    select
          claim_id
        , person_id
        , count(distinct encounter_id) as encounters_claim_overlaps_with
    from {{ ref('data_quality__prof_claims_overlapping_with_aip_encounters') }}
    group by
          claim_id
        , person_id

)

,  total_usable_aip_professional_claims as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as total
    from {{ ref('data_quality__prof_claims_overlapping_with_aip_encounters') }}

)

,  claims_overlapping_with_one_encounter as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as total
    from number_of_encounters_each_prof_claim_overlaps_with
    where encounters_claim_overlaps_with = 1

)

,  claims_overlapping_with_multiple_encounters as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as total
    from number_of_encounters_each_prof_claim_overlaps_with
    where encounters_claim_overlaps_with > 1

)

,  claims_overlapping_with_no_encounters as (

    select
          cast(count(distinct claim_id) as {{ dbt.type_numeric() }}) as total
    from number_of_encounters_each_prof_claim_overlaps_with
    where encounters_claim_overlaps_with = 0

)

, final as (

    select
        'Prof claims overlapping with one encounter' as field
        , claims_overlapping_with_one_encounter.total as number_of_claims
        , round(claims_overlapping_with_one_encounter.total * 100.0 / total_usable_aip_professional_claims.total, 1) as percent_of_usable_aip_prof_claims
    from claims_overlapping_with_one_encounter
    cross join total_usable_aip_professional_claims

    union all

    select
        'Prof claims overlapping with multiple encounters' as field
        , claims_overlapping_with_multiple_encounters.total as number_of_claims
        , round(claims_overlapping_with_multiple_encounters.total * 100.0 / total_usable_aip_professional_claims.total, 1) as percent_of_usable_aip_prof_claims
    from claims_overlapping_with_multiple_encounters
    cross join total_usable_aip_professional_claims

    union all

    select
        'Prof claims overlapping with no encounters' as field
        , claims_overlapping_with_no_encounters.total as number_of_claims
        , round(claims_overlapping_with_no_encounters.total * 100.0 / total_usable_aip_professional_claims.total, 1) as percent_of_usable_aip_prof_claims
    from claims_overlapping_with_no_encounters
    cross join total_usable_aip_professional_claims

)

select
      field
    , number_of_claims
    , percent_of_usable_aip_prof_claims
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final
