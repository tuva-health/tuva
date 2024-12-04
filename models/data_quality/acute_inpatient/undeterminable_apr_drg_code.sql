{{ config(
    enabled = var('claims_enabled', False)
) }}

with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('valid_apr_drg_code_counts') }}
    where valid_apr_drg_code_count > 1

)
, claims_with_multiple_valid_values_ranking_1 as (

    select
          hc.claim_id
        , hc.apr_drg_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('how_often_each_apr_drg_code_occurs') }} hc
    inner join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where hc.ranking = 1

)
, claims_with_multiple_valid_values_ranking_2 as (

    select
          hc.claim_id
        , hc.apr_drg_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('how_often_each_apr_drg_code_occurs') }} hc
    inner join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where hc.ranking = 2

)
, determinable as (

    select
          aa.claim_id
        , aa.apr_drg_code as apr_drg_code_1
        , aa.occurrences as occurrences_1
        , bb.apr_drg_code as apr_drg_code_2
        , bb.occurrences as occurrences_2
    from claims_with_multiple_valid_values_ranking_1 aa
    left join claims_with_multiple_valid_values_ranking_2 bb
      on aa.claim_id = bb.claim_id
    where aa.occurrences = bb.occurrences

)

select
      claim_id
    , apr_drg_code_1
    , occurrences_1
    , apr_drg_code_2
    , occurrences_2
from determinable
