{{ config(
    enabled = var('claims_enabled', False)
) }}

-- List of all unique claim_ids that have
-- null apr_drg_code on every line,
-- i.e. apr_drg_code is never populated:
with claims_with_missing_apr_drg_code as (

    select distinct
          claim_id
    from (
        select
              claim_id
            , max(apr_drg_code) as max_apr_drg_code
        from {{ ref('valid_values') }}
        group by
              claim_id
    )
    where max_apr_drg_code is null

)

, claim_lines_with_populated_apr_drg_code as (

    select
          claim_id
        , claim_line_number
        , apr_drg_code
        , valid_apr_drg_code
    from {{ ref('valid_values') }}
    where apr_drg_code is not null

)

-- This CTE is at the claim grain:
-- We have one row for each claim_id that has at least
-- one line with a populated (non null) apr_drg_code.
-- For each claim_id we have these flags:
--       always_valid_apr_drg_code = 1 when every line with a
--                                   populated apr_drg_code has
--                                   a valid apr_drg_code.
--       valid_and_invalid_apr_drg_code:   = 1 when the claim has valid apr_drg_code
--                                           populated on some lines and invalid
--                                           apr_drg_code
--                                           populated on some lines.
--       always_invalid_apr_drg_code: = 1 when every line with a populated
--                                          apr_drg_code has
--                                          an invalid
--                                          apr_drg_code.

, check_for_valid_values as (

    select
          claim_id
        , case
            when (  max(valid_apr_drg_code) = 1
                    and min(valid_apr_drg_code) = 1
                 ) then 1
            else 0
          end as always_valid_apr_drg_code
        , case
            when (  max(valid_apr_drg_code) = 1
                    and min(valid_apr_drg_code) = 0
                 ) then 1
            else 0
          end as valid_and_invalid_apr_drg_code
        , case
            when (  max(valid_apr_drg_code) = 0
                    and min(valid_apr_drg_code) = 0
                 ) then 1
            else 0
          end as always_invalid_apr_drg_code
    from claim_lines_with_populated_apr_drg_code aa
    group by
          claim_id

)

, all_claim_ids as (

    select
          claim_id
    from claims_with_missing_apr_drg_code

    union all

    select
          claim_id
    from check_for_valid_values

)

, apr_drg_code_summary as (

    select
          aa.claim_id as claim_id
        , case
            when bb.claim_id is not null then 1
            else 0
          end as missing_apr_drg_code
        , case
            when (cc.always_invalid_apr_drg_code = 1) then 1
            else 0
          end as always_invalid_apr_drg_code
        , case
            when (cc.valid_and_invalid_apr_drg_code = 1) then 1
            else 0
          end as valid_and_invalid_apr_drg_code
        , case
            when (cc.always_valid_apr_drg_code = 1) then 1
            else 0
          end as always_valid_apr_drg_code
        , case
            when (dd.assignment_method = 'unique') then 1
            else 0
          end as unique_apr_drg_code
        , case
            when (dd.assignment_method = 'determinable') then 1
            else 0
          end as determinable_apr_drg_code
        , case
            when (dd.assignment_method = 'undeterminable') then 1
            else 0
          end as undeterminable_apr_drg_code
        , case
            when (dd.assignment_method in ('unique', 'determinable')) then 1
            else 0
          end as usable_apr_drg_code
        , dd.apr_drg_code as assigned_apr_drg_code
    from all_claim_ids aa
    left join claims_with_missing_apr_drg_code bb
      on aa.claim_id = bb.claim_id
    left join check_for_valid_values cc
      on aa.claim_id = cc.claim_id
    left join {{ ref('assigned_apr_drg_code') }} dd
      on aa.claim_id = dd.claim_id

)

select
      claim_id
    , missing_apr_drg_code
    , always_invalid_apr_drg_code
    , valid_and_invalid_apr_drg_code
    , always_valid_apr_drg_code
    , unique_apr_drg_code
    , determinable_apr_drg_code
    , undeterminable_apr_drg_code
    , usable_apr_drg_code
    , assigned_apr_drg_code
from apr_drg_code_summary
