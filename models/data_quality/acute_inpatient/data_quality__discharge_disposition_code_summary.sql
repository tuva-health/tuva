{{ config(
    enabled = var('claims_enabled', False)
) }}

-- List of all unique claim_ids that have
-- null discharge_disposition_code on every line,
-- i.e. discharge_disposition_code is never populated,
-- i.e. discharge_disposition_code is missing:
with claims_with_missing_discharge_disposition_code as (

    select distinct
          claim_id
    from (
        select
              claim_id
            , max(discharge_disposition_code) as max_discharge_disposition_code
        from {{ ref('data_quality__valid_values') }}
        group by
              claim_id
    ) as max_discharge_disposition
    where max_discharge_disposition_code is null

)

-- All claim lines where discharge_disposition_code is populated:
, claim_lines_with_populated_discharge_disposition_code as (

    select
          claim_id
        , claim_line_number
        , discharge_disposition_code
        , valid_discharge_disposition_code
    from {{ ref('data_quality__valid_values') }}
    where discharge_disposition_code is not null

)

-- This CTE is at the claim grain:
-- We have one row for each claim_id that has at least
-- one line with a populated (non null) discharge_disposition_code.
-- For each claim_id we have these flags:
--       always_valid_discharge_disposition_code = 1 when every line with a
--                                   populated discharge_disposition_code has
--                                   a valid discharge_disposition_code.
--       valid_and_invalid_discharge_disposition_code:   = 1 when the claim has valid discharge_disposition_code
--                                           populated on some lines and invalid
--                                           discharge_disposition_code
--                                           populated on some lines.
--       always_invalid_discharge_disposition_code: = 1 when every line with a populated
--                                          discharge_disposition_code has
--                                          an invalid
--                                          discharge_disposition_code.

, check_for_valid_values as (

    select
          claim_id
        , case
            when (  max(valid_discharge_disposition_code) = 1
                    and min(valid_discharge_disposition_code) = 1
                 ) then 1
            else 0
          end as always_valid_discharge_disposition_code
        , case
            when (  max(valid_discharge_disposition_code) = 1
                    and min(valid_discharge_disposition_code) = 0
                 ) then 1
            else 0
          end as valid_and_invalid_discharge_disposition_code
        , case
            when (  max(valid_discharge_disposition_code) = 0
                    and min(valid_discharge_disposition_code) = 0
                 ) then 1
            else 0
          end as always_invalid_discharge_disposition_code
    from claim_lines_with_populated_discharge_disposition_code aa
    group by
          claim_id

)

, all_claim_ids as (

    select
          claim_id
    from claims_with_missing_discharge_disposition_code

    union all

    select
          claim_id
    from check_for_valid_values

)

, discharge_disposition_code_summary as (

    select
          aa.claim_id as claim_id
        , case
            when bb.claim_id is not null then 1
            else 0
          end as missing_discharge_disposition_code
        , case
            when (cc.always_invalid_discharge_disposition_code = 1) then 1
            else 0
          end as always_invalid_discharge_disposition_code
        , case
            when (cc.valid_and_invalid_discharge_disposition_code = 1) then 1
            else 0
          end as valid_and_invalid_discharge_disposition_code
        , case
            when (cc.always_valid_discharge_disposition_code = 1) then 1
            else 0
          end as always_valid_discharge_disposition_code
        , case
            when (dd.assignment_method = 'unique') then 1
            else 0
          end as unique_discharge_disposition_code
        , case
            when (dd.assignment_method = 'determinable') then 1
            else 0
          end as determinable_discharge_disposition_code
        , case
            when (dd.assignment_method = 'undeterminable') then 1
            else 0
          end as undeterminable_discharge_disposition_code
        , case
            when (dd.assignment_method in ('unique', 'determinable')) then 1
            else 0
          end as usable_discharge_disposition_code
        , dd.discharge_disposition_code as assigned_discharge_disposition_code
    from all_claim_ids aa
    left join claims_with_missing_discharge_disposition_code bb
      on aa.claim_id = bb.claim_id
    left join check_for_valid_values cc
      on aa.claim_id = cc.claim_id
    left join {{ ref('data_quality__assigned_discharge_disposition_code') }} dd
      on aa.claim_id = dd.claim_id

)

select
      claim_id
    , missing_discharge_disposition_code
    , always_invalid_discharge_disposition_code
    , valid_and_invalid_discharge_disposition_code
    , always_valid_discharge_disposition_code
    , unique_discharge_disposition_code
    , determinable_discharge_disposition_code
    , undeterminable_discharge_disposition_code
    , usable_discharge_disposition_code
    , assigned_discharge_disposition_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from discharge_disposition_code_summary
