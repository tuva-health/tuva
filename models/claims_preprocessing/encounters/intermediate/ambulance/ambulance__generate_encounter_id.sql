{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

with anchor as (
select distinct m.patient_data_source_id
 , m.data_source
 , m.start_date
 , m.claim_id
from {{ ref('encounters__stg_medical_claim') }} as m
inner join {{ ref('ambulance__anchor_events') }} as u on m.claim_id = u.claim_id
and
m.data_source = u.data_source
)

select patient_data_source_id
, data_source
, start_date
, claim_id
, {{ the_tuva_project.encounter_id_hash(["'ambulance'", 'patient_data_source_id', 'start_date']) }} as old_encounter_id
from anchor
