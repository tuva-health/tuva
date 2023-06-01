{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}


select
  patient_id
, paid_date
, ndc_code
, data_source
from {{ ref('core__pharmacy_claim') }}