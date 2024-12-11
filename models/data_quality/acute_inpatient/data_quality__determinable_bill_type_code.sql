{{ config(
    enabled = var('claims_enabled', False)
) }}

-- Claims with more than one valid bill_type_code:
with list_of_claims as (

    select distinct
          claim_id
    from {{ ref('data_quality__valid_bill_type_code_counts') }}
    where valid_bill_type_code_count > 1

)

, claims_with_multiple_valid_values_ranking_1 as (

    select
          claim_id
        , bill_type_code
        , occurrences
        , ranking
    from {{ ref('data_quality__how_often_each_bill_type_code_occurs') }}
    where ranking = 1
      and claim_id in (select * from list_of_claims)

)

, claims_with_multiple_valid_values_ranking_2 as (

    select
          claim_id
        , bill_type_code
        , occurrences
        , ranking
    from {{ ref('data_quality__how_often_each_bill_type_code_occurs') }}
    where ranking = 2
      and claim_id in (select * from list_of_claims)

)

, determinable as (

    select
          aa.claim_id
        , aa.bill_type_code as bill_type_code_1
        , aa.occurrences as occurrences_1
        , bb.bill_type_code as bill_type_code_2
        , bb.occurrences as occurrences_2
    from claims_with_multiple_valid_values_ranking_1 aa
    left join claims_with_multiple_valid_values_ranking_2 bb
      on aa.claim_id = bb.claim_id
    where aa.occurrences > bb.occurrences

)

select
      claim_id
    , bill_type_code_1
    , occurrences_1
    , bill_type_code_2
    , occurrences_2
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from determinable
