{{ config(
     enabled = var('financial_pmpm_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


SELECT
  patient_id
, year_month
, '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('financial_pmpm__member_months') }}