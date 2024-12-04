{{ config(
    enabled = var('claims_enabled', False)
) }}

-- List of all unique claim_ids that have
-- null bill_type_code on every line,
-- i.e., bill_type_code is never populated:
with claims_with_missing_bill_type_code as (

    select distinct
          claim_id
    from (
        select
              claim_id
            , max(bill_type_code) as max_bill_type_code
        from {{ ref('valid_values') }}
        group by
              claim_id
    )
    where max_bill_type_code is null

)

, claim_lines_with_populated_bill_type_code as (

    select
          claim_id
        , claim_line_number
        , bill_type_code
        , valid_bill_type_code
    from {{ ref('valid_values') }}
    where bill_type_code is not null

)

, check_for_valid_values as (

    select
          claim_id
        , case
            when (  max(valid_bill_type_code) = 1
                    and min(valid_bill_type_code) = 1
                 ) then 1
            else 0
          end as always_valid_bill_type_code
        , case
            when (  max(valid_bill_type_code) = 1
                    and min(valid_bill_type_code) = 0
                 ) then 1
            else 0
          end as valid_and_invalid_bill_type_code
        , case
            when (  max(valid_bill_type_code) = 0
                    and min(valid_bill_type_code) = 0
                 ) then 1
            else 0
          end as always_invalid_bill_type_code
    from claim_lines_with_populated_bill_type_code aa
    group by
          claim_id

)

, all_claim_ids as (

    select
          claim_id
    from claims_with_missing_bill_type_code

    union all

    select
          claim_id
    from check_for_valid_values

)

, bill_type_code_summary as (

    select
          aa.claim_id as claim_id
        , case
            when bb.claim_id is not null then 1
            else 0
          end as missing_bill_type_code
        , case
            when (cc.always_invalid_bill_type_code = 1) then 1
            else 0
          end as always_invalid_bill_type_code
        , case
            when (cc.valid_and_invalid_bill_type_code = 1) then 1
            else 0
          end as valid_and_invalid_bill_type_code
        , case
            when (cc.always_valid_bill_type_code = 1) then 1
            else 0
          end as always_valid_bill_type_code
        , case
            when (dd.assignment_method = 'unique') then 1
            else 0
          end as unique_bill_type_code
        , case
            when (dd.assignment_method = 'determinable') then 1
            else 0
          end as determinable_bill_type_code
        , case
            when (dd.assignment_method = 'undeterminable') then 1
            else 0
          end as undeterminable_bill_type_code
        , case
            when (dd.assignment_method in ('unique', 'determinable')) then 1
            else 0
          end as usable_bill_type_code
        , dd.bill_type_code as assigned_bill_type_code
    from all_claim_ids aa
    left join claims_with_missing_bill_type_code bb
      on aa.claim_id = bb.claim_id
    left join check_for_valid_values cc
      on aa.claim_id = cc.claim_id
    left join {{ ref('assigned_bill_type_code') }} dd
      on aa.claim_id = dd.claim_id

)

select
      claim_id
    , missing_bill_type_code
    , always_invalid_bill_type_code
    , valid_and_invalid_bill_type_code
    , always_valid_bill_type_code
    , unique_bill_type_code
    , determinable_bill_type_code
    , undeterminable_bill_type_code
    , usable_bill_type_code
    , assigned_bill_type_code
from bill_type_code_summary