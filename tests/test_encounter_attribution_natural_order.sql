{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false)
   )
}}

/*
  Encounter IDs are hashes in 1.0, so their lexical order must not decide which
  encounter receives an ambiguously matching claim line. Preserve the natural
  ordering that the former integer IDs represented for ED and ASC attribution.
*/

with ed_anchor as (
  select distinct
      encounter_id
    , original_anchor_claim
  from {{ ref('emergency_department__generate_encounter_id') }}
)

, ed_attribution as (
  select
      'emergency department' as encounter_type
    , attr.claim_id
    , attr.claim_line_number
    , attr.data_source
    , attr.claim_attribution_number
    , row_number() over (
        partition by attr.claim_id, attr.claim_line_number, attr.data_source
        order by
            anchor.original_anchor_claim
          , attr.encounter_id
          , attr.claim_attribution_number
      ) as expected_claim_attribution_number
  from {{ ref('emergency_department__prof_claims') }} as attr
  inner join ed_anchor as anchor
    on attr.encounter_id = anchor.encounter_id
)

, asc_attribution as (
  select
      'ambulatory surgery center' as encounter_type
    , claim_id
    , claim_line_number
    , data_source
    , claim_attribution_number
    , row_number() over (
        partition by claim_id, claim_line_number, data_source
        order by
            encounter_start_date
          , old_encounter_id
          , claim_attribution_number
      ) as expected_claim_attribution_number
  from {{ ref('asc__match_claims_to_anchor') }}
)

select *
from ed_attribution
where claim_attribution_number <> expected_claim_attribution_number

union all

select *
from asc_attribution
where claim_attribution_number <> expected_claim_attribution_number
