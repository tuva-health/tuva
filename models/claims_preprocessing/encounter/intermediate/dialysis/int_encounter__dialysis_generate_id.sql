{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with anchor as (
select distinct m.patient_data_source_id
 , m.start_date
 , m.claim_id
from {{ ref('int_encounter__claim_line') }} as m
inner join {{ ref('int_encounter__dialysis_anchor_event') }} as u on m.claim_id = u.claim_id
)

select patient_data_source_id
, start_date
, claim_id
, dense_rank() over (
order by patient_data_source_id, start_date) as old_encounter_id
from anchor
