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
    from {{ ref('all_line_level_room_and_board_rev_codes') }}
    group by claim_id

)

select *
from group_by_claim_id
