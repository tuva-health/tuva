{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , procedure_date
    , normalized_code_type as code_type
    , normalized_code as code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__procedure') }}