{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}


SELECT
  patient_id
, year_month
, '{{ var('last_update')}}' as last_update
from {{ ref('member_months') }}