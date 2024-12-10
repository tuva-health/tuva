{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      patient_id
    , claim_id
    , count(distinct encounter_id) as encounter_count
from {{ ref('aip_encounter_id') }}
group by
      patient_id
    , claim_id
having
    encounter_count > 1
