{{ config(
     enabled = var('quality_measures_reporting_enabled',var('tuva_marts_enabled',True))
   )
}}
select
      patient_id
    , dispensing_date
    , ndc_code
    , paid_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('pharmacy_claim') }}