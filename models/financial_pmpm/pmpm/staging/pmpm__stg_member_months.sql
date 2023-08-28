{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  patient_id
, year_month
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('member_months__member_months') }}