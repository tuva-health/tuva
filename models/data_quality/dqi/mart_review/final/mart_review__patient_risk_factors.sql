{{ config(
     enabled = (var('enable_legacy_data_quality', false) | as_bool)
     and (var('claims_enabled', False) | as_bool)
   )
}}

select *
,dense_rank() over (
order by person_id, model_version, payment_year ) as patient_risk_sk
from {{ ref('cms_hcc__patient_risk_factors') }} as p
