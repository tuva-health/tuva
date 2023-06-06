{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

select patient_id
from {{ ref('core__patient') }}