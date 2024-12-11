{{ config(
    enabled = var('claims_enabled', False)
) }}

with add_count_of_occurrences as (

    select
          claim_id
        , admit_source_code
        , count(*) as occurrences
    from {{ ref('data_quality__valid_values') }}
    where valid_admit_source_code = 1
    group by
          claim_id
        , admit_source_code

)

, add_ranking_from_high_to_low_occurrences as (

    select
          claim_id
        , admit_source_code
        , occurrences
        , row_number() over (
            partition by claim_id
            order by occurrences desc
        ) as ranking
    from add_count_of_occurrences

)

select
      claim_id
    , admit_source_code
    , occurrences
    , ranking
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_ranking_from_high_to_low_occurrences
