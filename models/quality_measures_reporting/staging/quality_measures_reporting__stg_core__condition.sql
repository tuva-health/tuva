{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , claim_id
    , condition_date
    , code_type
    , code
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__condition') }}