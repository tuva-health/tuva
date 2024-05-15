{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select 
    data_source
  , birth_date
  , patient_id
  , sex
from 
    {{ ref('core__patient') }}

