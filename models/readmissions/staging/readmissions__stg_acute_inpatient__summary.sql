{{ config(
     enabled = var('readmissions_enabled',var('tuva_marts_enabled',True))
   )
}}

select
    encounter_id,
    patient_id,
    encounter_start_date,
    encounter_end_date,
    discharge_disposition_code,
    facility_npi,
    ms_drg_code,
    total_paid_amount,
    '{{ var('last_update')}}' as last_update
from {{ ref('acute_inpatient__summary') }}
