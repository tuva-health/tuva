{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('medical_records_enabled',var('tuva_marts_enabled',False)))
   )
}}

select 
    patient_id
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__patient') }}