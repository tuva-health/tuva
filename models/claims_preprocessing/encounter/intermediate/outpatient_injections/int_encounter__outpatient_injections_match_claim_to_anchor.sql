{{ config(
     enabled = (var('claims_enabled', False) | string | lower) == 'true'
   )
}}

select distinct m.patient_data_source_id
 , m.start_date
 , m.claim_id
 , m.claim_line_number
 , u.old_encounter_id
from {{ ref('int_encounter__claim_line') }} as m
inner join {{ ref('int_encounter__outpatient_injections_generate_id') }} as u on m.patient_data_source_id = u.patient_data_source_id
and
m.start_date = u.start_date
