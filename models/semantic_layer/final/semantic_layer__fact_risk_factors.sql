{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    rf.person_id
  , rf.factor_type
  , rf.risk_factor_description
  , rf.coefficient
  , rf.model_version
  , rf.payment_year
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('semantic_layer__stg_cms_hcc__patient_risk_factors') }} as rf