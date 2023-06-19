{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
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
, admit_source_code
, admit_type_code
, discharge_disposition_code
, facility_npi
, ms_drg_code
, apr_drg_code
, total_paid_amount
, total_allowed_amount
, total_charge_amount
, data_source
, '{{ var('last_update')}}' as last_update
from {{ ref('acute_inpatient__summary') }} 
