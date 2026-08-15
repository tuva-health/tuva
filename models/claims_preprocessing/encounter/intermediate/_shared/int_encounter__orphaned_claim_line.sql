{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

with cte as (
  select stg.claim_id
  , stg.claim_line_number
  , stg.service_category_1
  , stg.service_category_2
  , stg.service_category_3
  , stg.claim_type
  , stg.claim_start_date
  , stg.claim_end_date
  , stg.start_date
  , stg.end_date
  , stg.patient_data_source_id
  from {{ ref('int_encounter__claim_line') }} as stg
  left outer join {{ ref('int_encounter__combined_claim_line_crosswalk') }} as enc on stg.claim_id = enc.claim_id
  and
  stg.claim_line_number = enc.claim_line_number
  where enc.claim_id is null -- missing from encounter mapping table
)

select
  claim_id
, claim_line_number
/* Orphan ids no longer need offsetting past max(encounter_id). That offset
   existed only because sequential integer ids would otherwise collide with the
   crosswalk's. A surrogate key is derived from the row's own natural key, so it
   cannot collide with keys built from a different column set. Dropping it also
   removes a max() over the entire crosswalk and the cross join. */
, {{ dbt_utils.generate_surrogate_key(['patient_data_source_id', 'claim_id']) }} as encounter_id
, 'orphaned claim' as encounter_type
, 'other' as encounter_group
from cte
