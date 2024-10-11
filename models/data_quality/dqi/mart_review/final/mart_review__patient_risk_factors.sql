{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

select *
,dense_rank() over (order by patient_id, model_version, payment_year ) as patient_risk_sk
from {{ ref('cms_hcc__patient_risk_factors') }} p