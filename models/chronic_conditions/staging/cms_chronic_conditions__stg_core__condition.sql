{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}


select
  encounter_id
, code
, code_type
from {{ ref('core__condition') }}