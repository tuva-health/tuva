{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
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
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('medical_claim') }}