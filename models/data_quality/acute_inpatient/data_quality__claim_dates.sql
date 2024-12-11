{{ config(
    enabled = var('claims_enabled', False)
) }}

-- If a date is not valid (i.e. does not have a valid flag)
-- then we replace that date by null:
with get_all_valid_raw_dates as (
    select
          claim_id
        , claim_line_number
        
        , case
            when valid_claim_start_date = 1 then claim_start_date
            else null
          end as claim_start_date

        , case
            when valid_claim_end_date = 1 then claim_end_date
            else null
          end as claim_end_date

        , case
            when valid_admission_date = 1 then admission_date
            else null
          end as admission_date

        , case
            when valid_discharge_date = 1 then discharge_date
            else null
          end as discharge_date

        , case
            when valid_claim_line_start_date = 1 then claim_line_start_date
            else null
          end as claim_line_start_date

        , case
            when valid_claim_line_end_date = 1 then claim_line_end_date
            else null
          end as claim_line_end_date

    from {{ ref('data_quality__valid_values') }}
),

-- Here we define 3 new useful dates:
--     [1] merge_start_date: The start date for a claim that we use when
--                           we potentially merge that claim with other claims
--                           into a single encounter.
--     [2] merge_end_date:   The end date for a claim that we use when
--                           we potentially merge that claim with other claims
--                           into a single encounter.
--     [3] dx_date:          The date we assign to diagnosis codes present
--                           on that claim.
define_new_useful_dates as (
    select
          claim_id
        , -- this is the start date of the claim we use for merging the claim
          -- to other claims to form encounters:
          coalesce(
              min(admission_date),
              min(claim_start_date),
              min(claim_line_start_date)
          ) as merge_start_date

        , -- this is the end date of the claim we use for merging the claim
          -- to other claims to form encounters:
          coalesce(
              max(discharge_date),
              max(claim_end_date),
              max(claim_line_end_date)
          ) as merge_end_date

        , -- this is the date we assign to diagnosis codes on the claim:
          coalesce(
              max(claim_end_date),
              max(claim_start_date),
              max(discharge_date),
              max(admission_date),
              max(claim_line_end_date),
              max(claim_line_start_date)
          ) as dx_date
    from get_all_valid_raw_dates 
    group by claim_id
),

flags_for_merge_start_and_end_dates as (
    select
          claim_id
        , merge_start_date
        , merge_end_date
        , dx_date
        , case
            when merge_start_date > merge_end_date then 1
            else 0
          end as merge_start_date_after_merge_end_date
        , case
            when (
                    (merge_start_date <= merge_end_date) and
                    (merge_start_date is not null) and
                    (merge_end_date is not null)
                ) then 1
            else 0
          end as usable_merge_dates
    from define_new_useful_dates
),

final_dates_table as (
    select
          aa.claim_id
        , aa.claim_line_number

        , aa.claim_start_date
        , aa.claim_end_date

        , aa.admission_date
        , aa.discharge_date

        , aa.claim_line_start_date
        , aa.claim_line_end_date

        , bb.merge_start_date
        , bb.merge_end_date
        , bb.merge_start_date_after_merge_end_date
        , bb.usable_merge_dates
        , bb.dx_date
    from get_all_valid_raw_dates aa
    left join flags_for_merge_start_and_end_dates bb
        on aa.claim_id = bb.claim_id
)

SELECT
      claim_id
    , claim_line_number

    , claim_start_date
    , claim_end_date

    , admission_date
    , discharge_date

    , claim_line_start_date
    , claim_line_end_date

    , merge_start_date
    , merge_end_date
    , merge_start_date_after_merge_end_date
    , usable_merge_dates
    , dx_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
FROM final_dates_table
