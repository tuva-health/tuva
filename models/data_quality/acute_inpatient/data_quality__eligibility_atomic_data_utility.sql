{{ config(
    enabled = var('claims_enabled', False)
) }}

with final as (
    select
          field
        , missing_count
        , missing_perc
        , invalid_count
        , invalid_perc
        , duplicated_count
        , duplicated_perc
        , claim_type
    from {{ ref('data_quality__eligibility_atomic_data_utility_stage_01') }}

    union all

    select
          field
        , missing_count
        , missing_perc
        , invalid_count
        , invalid_perc
        , duplicated_count
        , duplicated_perc
        , claim_type
    from {{ ref('data_quality__eligibility_atomic_data_utility_stage_02') }}

)

select
      'eligibility' as table_name
    , field
    , claim_type
    , missing_count
    , missing_perc
    , invalid_count
    , invalid_perc
    , duplicated_count
    , duplicated_perc
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from final