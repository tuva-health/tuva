{{ config(
    enabled = var('claims_enabled', False)
) }}

-- Claims with more than one valid drg_code:
with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('data_quality__valid_drg_code_counts') }}
    where valid_drg_code_count > 1

)

, claims_with_multiple_valid_values_ranking_1 as (

    select
          hc.claim_id
        , hc.drg_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('data_quality__how_often_each_drg_code_occurs') }} hc
    left join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where lc.claim_id is not null
      and hc.ranking = 1

)

, claims_with_multiple_valid_values_ranking_2 as (

    select
          hc.claim_id
        , hc.drg_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('data_quality__how_often_each_drg_code_occurs') }} hc
    left join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where lc.claim_id is not null
      and hc.ranking = 2

)

, determinable as (

    select
          aa.claim_id
        , aa.drg_code as drg_code_1
        , aa.occurrences as occurrences_1
        , bb.drg_code as drg_code_2
        , bb.occurrences as occurrences_2
    from claims_with_multiple_valid_values_ranking_1 aa
    left join claims_with_multiple_valid_values_ranking_2 bb
      on aa.claim_id = bb.claim_id
    where aa.occurrences = bb.occurrences

)

select
      claim_id
    , drg_code_1
    , occurrences_1
    , drg_code_2
    , occurrences_2
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from determinable
