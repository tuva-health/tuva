{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}


select *
from {{ ref('cms_hcc__patient_risk_scores') }} as p
