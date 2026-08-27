{{ config(
     enabled = the_tuva_project.tuva_boolean_var('claims_enabled', false),
     severity = 'error',
     tags = ['claims_preprocessing', 'encounter_contract']
   )
}}

{#
  Broad encounter matchers can produce same-day candidate assignments without
  consulting claim_type. Undetermined claims must remain unclassified so they
  flow through the orphan fallback and do not acquire claim-line encounter
  mappings used by Core condition and procedure.
#}

select
      crosswalk.claim_id
    , crosswalk.claim_line_number
    , crosswalk.data_source
    , crosswalk.encounter_id
    , crosswalk.encounter_type
from {{ ref('encounters__combined_claim_line_crosswalk') }} as crosswalk
inner join {{ ref('encounters__stg_medical_claim') }} as staged_claim
    on crosswalk.claim_id = staged_claim.claim_id
    and crosswalk.claim_line_number = staged_claim.claim_line_number
    and crosswalk.data_source = staged_claim.data_source
where staged_claim.claim_type = 'undetermined'
