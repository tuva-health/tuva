{{ config(
     enabled = var('tuva_chronic_conditions_enabled',var('medical_records_enabled',var('tuva_marts_enabled',False)))
   )
}}

select 
      patient_id
    , normalized_code
    , recorded_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__condition')}}