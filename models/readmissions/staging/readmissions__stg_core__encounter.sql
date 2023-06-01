{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
    encounter_id,
    patient_id,
    encounter_start_date,
    encounter_end_date,
    encounter_type,
    discharge_disposition_code,
    facility_npi,
    ms_drg_code,
    paid_amount    
from {{ ref('core__encounter') }}
