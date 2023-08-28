{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the encounter table in core.
-- *************************************************

select
  encounter_id
, patient_id
, 'acute inpatient' as encounter_type
, encounter_start_date
, encounter_end_date
, length_of_stay
, admit_source_code
, admit_source_description
, admit_type_code
, admit_type_description
, discharge_disposition_code
, discharge_disposition_description
, null as attending_provider_id
, facility_npi
, null as primary_diagnosis_code
, null as primary_diagnosis_description
, ms_drg_code
, ms_drg_description
, apr_drg_code
, apr_drg_description
, total_paid_amount as paid_amount
, total_allowed_amount as allowed_amount
, total_charge_amount as charge_amount
, data_source
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('acute_inpatient__summary') }} 
