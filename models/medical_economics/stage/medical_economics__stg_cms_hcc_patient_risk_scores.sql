{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

select 
    person_id
  , payment_year
  , payment_risk_score 
from 
    {{ ref('cms_hcc__patient_risk_scores') }} 
