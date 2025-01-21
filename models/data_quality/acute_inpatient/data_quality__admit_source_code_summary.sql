{{ config(
    enabled = var('claims_enabled', False)
) }}

-- List of all unique claim_ids that have
-- null admit_source_code on every line,
-- i.e. admit_source_code is never populated,
-- i.e. admit_source_code is missing:

with claims_with_missing_admit_source_code as (

    select distinct
          claim_id
    from (
        select
              claim_id
            , max(admit_source_code) as max_admit_source_code
        from {{ ref('data_quality__valid_values') }}
        group by
              claim_id
    ) as max_admit_source
    where max_admit_source_code is null

)

-- All claim lines where admit_source_code is populated:

, claim_lines_with_populated_admit_source_code as (

    select
          claim_id
        , claim_line_number
        , admit_source_code
        , valid_admit_source_code
    from {{ ref('data_quality__valid_values') }}
    where admit_source_code is not null

)

-- This CTE is at the claim grain:
-- We have one row for each claim_id that has at least
-- one line with a populated (non null) admit_source_code.
-- For each claim_id we have these flags:
--       always_valid_admit_source_code = 1 when every line with a
--                                   populated admit_source_code has
--                                   a valid admit_source_code.
--       valid_and_invalid_admit_source_code:   = 1 when the claim has valid admit_source_code
--                                           populated on some lines and invalid
--                                           admit_source_code
--                                           populated on some lines.
--       always_invalid_admit_source_code: = 1 when every line with a populated
--                                          admit_source_code has
--                                          an invalid
--                                          admit_source_code.

, check_for_valid_values as (

    select
          claim_id
        , case
            when (  max(valid_admit_source_code) = 1
                    and min(valid_admit_source_code) = 1
                 ) then 1
            else 0
          end as always_valid_admit_source_code
        , case
            when (  max(valid_admit_source_code) = 1
                    and min(valid_admit_source_code) = 0
                 ) then 1
            else 0
          end as valid_and_invalid_admit_source_code
        , case
            when (  max(valid_admit_source_code) = 0
                    and min(valid_admit_source_code) = 0
                 ) then 1
            else 0
          end as always_invalid_admit_source_code
    from claim_lines_with_populated_admit_source_code aa
    group by
          claim_id

)

, all_claim_ids as (

    select
          claim_id
    from claims_with_missing_admit_source_code

    union all

    select
          claim_id
    from check_for_valid_values

)

, admit_source_code_summary as (

    select
          aa.claim_id as claim_id
        , case
            when bb.claim_id is not null then 1
            else 0
          end as missing_admit_source_code
        , case
            when (cc.always_invalid_admit_source_code = 1) then 1
            else 0
          end as always_invalid_admit_source_code
        , case
            when (cc.valid_and_invalid_admit_source_code = 1) then 1
            else 0
          end as valid_and_invalid_admit_source_code
        , case
            when (cc.always_valid_admit_source_code = 1) then 1
            else 0
          end as always_valid_admit_source_code
        , case
            when (dd.assignment_method = 'unique') then 1
            else 0
          end as unique_admit_source_code
        , case
            when (dd.assignment_method = 'determinable') then 1
            else 0
          end as determinable_admit_source_code
        , case
            when (dd.assignment_method = 'undeterminable') then 1
            else 0
          end as undeterminable_admit_source_code
        , case
            when (dd.assignment_method in ('unique', 'determinable')) then 1
            else 0
          end as usable_admit_source_code
        , dd.admit_source_code as assigned_admit_source_code
    from all_claim_ids aa
    left join claims_with_missing_admit_source_code bb
      on aa.claim_id = bb.claim_id
    left join check_for_valid_values cc
      on aa.claim_id = cc.claim_id
    left join {{ ref('data_quality__assigned_admit_source_code') }} dd
      on aa.claim_id = dd.claim_id

)

select
      claim_id
    , missing_admit_source_code
    , always_invalid_admit_source_code
    , valid_and_invalid_admit_source_code
    , always_valid_admit_source_code
    , unique_admit_source_code
    , determinable_admit_source_code
    , undeterminable_admit_source_code
    , usable_admit_source_code
    , assigned_admit_source_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from admit_source_code_summary
