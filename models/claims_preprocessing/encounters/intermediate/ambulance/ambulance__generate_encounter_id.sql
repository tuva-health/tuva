{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with anchor as (
select distinct m.patient_data_source_id
 , m.start_date
 , m.claim_id
from {{ ref('encounters__stg_medical_claim') }} as m
inner join {{ ref('ambulance__anchor_events') }} as u on m.claim_id = u.claim_id
)

select patient_data_source_id
, start_date
, claim_id
, dense_rank() over (
order by patient_data_source_id, start_date) as old_encounter_id
from anchor
