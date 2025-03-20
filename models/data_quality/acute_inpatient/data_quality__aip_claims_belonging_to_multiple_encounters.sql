{{ config(
    enabled = var('claims_enabled', False)
) }}

select
      person_id
    , claim_id
    , count(distinct encounter_id) as encounter_count
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('data_quality__aip_encounter_id') }}
group by
      person_id
    , claim_id
having
    count(distinct encounter_id) > 1
