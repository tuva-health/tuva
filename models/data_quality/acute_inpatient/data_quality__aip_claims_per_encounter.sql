{{ config(
    enabled = var('claims_enabled', False)
) }}

with claims_per_encounter as (

    select
          person_id
        , encounter_id
        , count(distinct claim_id) as claims
    from {{ ref('data_quality__aip_multiple_claim_encounters') }}
    where usable_for_aip_encounter = 1
    group by 
          person_id
        , encounter_id

)

, summary as (

    select
          claims as number_of_claims_in_encounter
        , count(*) as number_of_times_this_happens
    from claims_per_encounter
    group by 
          claims

)

select 
      number_of_claims_in_encounter
    , number_of_times_this_happens
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from summary
