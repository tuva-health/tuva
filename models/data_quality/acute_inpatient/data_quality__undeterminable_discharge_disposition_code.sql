{{ config(
    enabled = var('claims_enabled', False)
) }}

-- Claims with more than one valid discharge_disposition_code:
with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('data_quality__valid_discharge_disposition_code_counts') }}
    where valid_discharge_disposition_code_count > 1

)

, claims_with_multiple_valid_values_ranking_1 as (

    select
          hc.claim_id
        , hc.discharge_disposition_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('data_quality__how_often_each_discharge_disposition_code_occurs') }} hc
    inner join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where hc.ranking = 1

)

, claims_with_multiple_valid_values_ranking_2 as (

    select
          hc.claim_id
        , hc.discharge_disposition_code
        , hc.occurrences
        , hc.ranking
    from {{ ref('data_quality__how_often_each_discharge_disposition_code_occurs') }} hc
    inner join list_of_claims lc
      on hc.claim_id = lc.claim_id
    where hc.ranking = 2

)

, determinable as (

    select
          aa.claim_id
        , aa.discharge_disposition_code as discharge_disposition_code_1
        , aa.occurrences as occurrences_1
        , bb.discharge_disposition_code as discharge_disposition_code_2
        , bb.occurrences as occurrences_2
    from claims_with_multiple_valid_values_ranking_1 aa
    left join claims_with_multiple_valid_values_ranking_2 bb
      on aa.claim_id = bb.claim_id
    where aa.occurrences = bb.occurrences

)

select
      claim_id
    , discharge_disposition_code_1
    , occurrences_1
    , discharge_disposition_code_2
    , occurrences_2
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from determinable

