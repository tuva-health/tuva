{{ config(
    enabled = var('claims_enabled', False)
) }}

-- Claims with more than one valid diagnosis_code_1:
with list_of_claims as (
    select distinct claim_id
    from {{ ref('data_quality__valid_diagnosis_code_1_counts') }}
    where valid_diagnosis_code_1_count > 1
)

, claims_with_multiple_valid_values_ranking_1 as (
    select
          claim_id
        , diagnosis_code_1
        , occurrences
        , ranking
    from {{ ref('data_quality__how_often_each_diagnosis_code_1_occurs') }}
    where ranking = 1
      and claim_id in (select claim_id from list_of_claims)
)

, claims_with_multiple_valid_values_ranking_2 as (
    select
          claim_id
        , diagnosis_code_1
        , occurrences
        , ranking
    from {{ ref('data_quality__how_often_each_diagnosis_code_1_occurs') }}
    where ranking = 2
      and claim_id in (select claim_id from list_of_claims)
)

, determinable as (
    select
          aa.claim_id
        , aa.diagnosis_code_1 as diagnosis_code_1_1
        , aa.occurrences as occurrences_1
        , bb.diagnosis_code_1 as diagnosis_code_1_2
        , bb.occurrences as occurrences_2
    from claims_with_multiple_valid_values_ranking_1 aa
    left join claims_with_multiple_valid_values_ranking_2 bb
      on aa.claim_id = bb.claim_id
    where aa.occurrences > bb.occurrences
)

select
      claim_id
    , diagnosis_code_1_1
    , occurrences_1
    , diagnosis_code_1_2
    , occurrences_2
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from determinable
