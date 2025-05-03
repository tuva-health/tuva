{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select distinct m.patient_data_source_id
 , m.start_date
 , m.claim_id
 , m.claim_line_number
 , u.old_encounter_id
from {{ ref('encounters__stg_medical_claim') }} as m
inner join {{ ref('outpatient_injections__generate_encounter_id') }} as u on m.patient_data_source_id = u.patient_data_source_id
and
m.start_date = u.start_date
