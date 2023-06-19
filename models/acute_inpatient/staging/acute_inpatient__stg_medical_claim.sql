{{ config(
     enabled = var('acute_inpatient_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
  claim_id
, claim_line_number
, patient_id
, claim_type
, claim_start_date
, claim_end_date
, admission_date
, discharge_date
, facility_npi
, ms_drg_code
, apr_drg_code
, admit_source_code
, admit_type_code
, discharge_disposition_code
, paid_amount
, allowed_amount
, charge_amount
, data_source
, '{{ var('last_update')}}' as last_update
from {{ ref('medical_claim') }}