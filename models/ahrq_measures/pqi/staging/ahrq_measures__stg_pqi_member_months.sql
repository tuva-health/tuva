{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

select 
    data_source
  , patient_id
  , year_month
from 
    {{ ref('financial_pmpm__member_months') }}

