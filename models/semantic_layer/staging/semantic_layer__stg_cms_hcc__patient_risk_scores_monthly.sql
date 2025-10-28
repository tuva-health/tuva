{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    rsm.collection_end_date
  , rsm.person_id
  , rsm.normalized_risk_score
FROM {{ ref('cms_hcc__patient_risk_scores_monthly') }} as rsm