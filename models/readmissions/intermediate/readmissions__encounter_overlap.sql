{{ config(
     enabled = var('readmissions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}


with encounter_enhanced as (
  select
    *
    -- Calculate actual length of stay in days
    , COALESCE(
      case
        when discharge_date >= admit_date
        then discharge_date - admit_date + 1
        else 1
      end
      , 1
    ) as actual_length_of_stay

    -- Source type priority (lower number = higher priority)
    , case
      when UPPER(TRIM(COALESCE(encounter_source_type, ''))) = 'CLAIM' then 1
      else 2
    end as source_type_priority

    -- Data completeness score (count of complete fields, higher = better)
    , (case when discharge_disposition_code is not null
          and TRIM(discharge_disposition_code) != ''
          and TRIM(discharge_disposition_code) != '00' then 1 else 0 end) +
    (case when drg_code_type is not null
          and TRIM(drg_code_type) != '' then 1 else 0 end) +
    (case when drg_code is not null
          and TRIM(drg_code) != ''
          and TRIM(drg_code) not in ('998', '999') then 1 else 0 end) +
    (case when paid_amount is not null then 1 else 0 end) +
    (case when primary_diagnosis_code is not null
          and TRIM(primary_diagnosis_code) != '' then 1 else 0 end) as completeness_score
  from {{ ref('readmissions__encounter') }}
)

-- Identify all encounters that have overlapping dates with other encounters for the same person
, overlapping_encounters as (
  select distinct
    e1.encounter_id
    , e1.person_id
    , e1.admit_date
    , e1.discharge_date
  from encounter_enhanced as e1
  where exists (
    select 1
    from encounter_enhanced as e2
    where e1.person_id = e2.person_id
      and e1.encounter_id != e2.encounter_id
      and e1.admit_date <= e2.discharge_date
      and e1.discharge_date >= e2.admit_date
  )
)

-- Create overlap groups by assigning a group identifier
, overlap_groups as (
  select
    e1.encounter_id
    , e1.person_id
    , MIN(e2.encounter_id) over (
      partition by e1.person_id, e1.encounter_id
    ) as overlap_group_id
  from overlapping_encounters as e1
  inner join overlapping_encounters as e2
    on e1.person_id = e2.person_id
    and e1.admit_date <= e2.discharge_date
    and e1.discharge_date >= e2.admit_date
)

-- Rank encounters within each overlap group
, encounter_rankings as (
  select
    e.*
    , COALESCE(og.overlap_group_id, e.encounter_id) as overlap_group_id
    , case when og.encounter_id is not null then 1 else 0 end as has_overlaps
    , ROW_NUMBER() over (
      partition by e.person_id, COALESCE(og.overlap_group_id, e.encounter_id)
      order by
        e.source_type_priority asc        -- prefer 'claim' source type
        , e.actual_length_of_stay desc      -- prefer longer date spans
        , e.completeness_score desc         -- prefer more complete data
        , e.encounter_id asc                 -- Consistent tie-breaker
    ) as encounter_rank_in_group
  from encounter_enhanced as e
  left outer join overlap_groups as og
    on e.encounter_id = og.encounter_id
    and e.person_id = og.person_id
)


select
  encounter_id
  , person_id
  , admit_date
  , discharge_date
  , actual_length_of_stay
  , source_type_priority
  , completeness_score
  , overlap_group_id
  , has_overlaps
  , encounter_rank_in_group
  , case
    when encounter_rank_in_group = 1 then 1
    else 0
  end as is_best_encounter

  , case
    when encounter_rank_in_group = 1 and has_overlaps = 1 then 'Selected as best among overlapping encounters'
    when encounter_rank_in_group = 1 and has_overlaps = 0 then 'No overlapping encounters'
    else 'Not selected - better encounter exists'
  end as selection_reason

from encounter_rankings
