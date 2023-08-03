{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , gender
    , birth_date
    , death_date
    , '{{ var('last_update')}}' as last_update
from {{ ref('core__patient') }}