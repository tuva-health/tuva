{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}


select
  patient_id
, encounter_id
, encounter_start_date
, ms_drg_code
, data_source
from {{ ref('core__encounter') }}