{{ config(
    enabled = var('claims_enabled', False)
) }}

with claims_with_missing_diagnosis_code_1 as (
    select distinct claim_id
    from (
        select
              claim_id
            , max(diagnosis_code_1) as max_diagnosis_code_1
        from {{ ref('data_quality__valid_values') }}
        group by claim_id
    ) as max_diag_code
    where max_diagnosis_code_1 is null
)

, claim_lines_with_populated_diagnosis_code_1 as (
    select
          claim_id
        , claim_line_number
        , diagnosis_code_1
        , valid_diagnosis_code_1
    from {{ ref('data_quality__valid_values') }}
    where diagnosis_code_1 is not null
)

, check_for_valid_values as (
    select
          claim_id
        , case
            when (  max(valid_diagnosis_code_1) = 1
                    and min(valid_diagnosis_code_1) = 1
                  ) then 1
            else 0
          end as always_valid_diagnosis_code_1
        , case
            when (  max(valid_diagnosis_code_1) = 1
                    and min(valid_diagnosis_code_1) = 0
                  ) then 1
            else 0
          end as valid_and_invalid_diagnosis_code_1
        , case
            when (  max(valid_diagnosis_code_1) = 0
                    and min(valid_diagnosis_code_1) = 0
                  ) then 1
            else 0
          end as always_invalid_diagnosis_code_1
    from claim_lines_with_populated_diagnosis_code_1 aa
    group by claim_id
)

, all_claim_ids as (
    select claim_id from claims_with_missing_diagnosis_code_1
    union all
    select claim_id from check_for_valid_values
)

, diagnosis_code_1_summary as (
    select
          aa.claim_id as claim_id
        , case
            when bb.claim_id is not null then 1
            else 0
          end as missing_diagnosis_code_1
        , case
            when (cc.always_invalid_diagnosis_code_1 = 1) then 1
            else 0
          end as always_invalid_diagnosis_code_1
        , case
            when (cc.valid_and_invalid_diagnosis_code_1 = 1) then 1
            else 0
          end as valid_and_invalid_diagnosis_code_1
        , case
            when (cc.always_valid_diagnosis_code_1 = 1) then 1
            else 0
          end as always_valid_diagnosis_code_1
        , case
            when (dd.assignment_method = 'unique') then 1
            else 0
          end as unique_diagnosis_code_1
        , case
            when (dd.assignment_method = 'determinable') then 1
            else 0
          end as determinable_diagnosis_code_1
        , case
            when (dd.assignment_method = 'undeterminable') then 1
            else 0
          end as undeterminable_diagnosis_code_1
        , case
            when (dd.assignment_method in ('unique','determinable')) then 1
            else 0
          end as usable_diagnosis_code_1
        , dd.diagnosis_code_1 as assigned_diagnosis_code_1
    from all_claim_ids aa
    left join claims_with_missing_diagnosis_code_1 bb
      on aa.claim_id = bb.claim_id
    left join check_for_valid_values cc
      on aa.claim_id = cc.claim_id
    left join {{ ref('data_quality__assigned_diagnosis_code_1') }} dd
      on aa.claim_id = dd.claim_id
)

select
      claim_id
    , missing_diagnosis_code_1
    , always_invalid_diagnosis_code_1
    , valid_and_invalid_diagnosis_code_1
    , always_valid_diagnosis_code_1
    , unique_diagnosis_code_1
    , determinable_diagnosis_code_1
    , undeterminable_diagnosis_code_1
    , usable_diagnosis_code_1
    , assigned_diagnosis_code_1
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from diagnosis_code_1_summary
