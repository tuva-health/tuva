with cte as (
  select stg.claim_id
  ,stg.claim_line_number
  ,stg.service_category_1
  ,stg.service_category_2
  ,stg.service_category_3
  ,stg.claim_type
  ,stg.claim_start_date
  ,stg.claim_end_date
  ,stg.start_date
  ,stg.end_date
  ,stg.patient_id
  from {{ ref('encounters__stg_medical_claim') }} stg
  left join  {{ ref('encounters__combined_claim_line_crosswalk') }} enc on stg.claim_id = enc.claim_id
  and
  stg.claim_line_number = enc.claim_line_number
  where enc.claim_id is null -- missing from encounter mapping table
)

,max_encounter as (
  select max(encounter_id) max_encounter_id
  from {{ ref('encounters__combined_claim_line_crosswalk') }}
)

select 
  claim_id
, claim_line_number
, dense_rank() over (order by patient_id, claim_id) + max_encounter.max_encounter_id as encounter_id
, 'orphaned claim' as encounter_type
, 'other' as encounter_group
from cte
cross join max_encounter