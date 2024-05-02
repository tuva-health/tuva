{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select 
    *
  , to_char(encounter_start_date, 'yyyy') as year_number
from 
    {{ ref('core__encounter') }}
where 
    encounter_type = 'acute inpatient'
