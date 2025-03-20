{{ config(
    enabled = var('claims_enabled', False)
) }}

with group_by_claim_id as (

    select
          claim_id
        , count(distinct revenue_center_code) as distinct_rev_code_count
        , case
            when max(valid_revenue_center_code) = 1 then 1
            else 0
          end as has_a_valid_rev_code
        , case
            when min(valid_revenue_center_code) = 0 then 1
            else 0
          end as has_an_invalid_rev_code
        , max(basic) as basic
        , max(hospice) as hospice
        , max(loa) as loa
        , max(behavioral) as behavioral
    from {{ ref('data_quality__all_line_level_room_and_board_rev_codes') }}
    group by claim_id

)

select
      claim_id
    , distinct_rev_code_count
    , has_a_valid_rev_code
    , has_an_invalid_rev_code
    , basic
    , hospice
    , loa
    , behavioral
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from group_by_claim_id
