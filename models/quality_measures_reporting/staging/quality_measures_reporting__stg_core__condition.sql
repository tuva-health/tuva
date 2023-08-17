{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , claim_id
    , recorded_date as condition_date
    , normalized_code_type as code_type
    , normalized_code as code
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__condition') }}