{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

select 
    patient_id
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__patient') }}