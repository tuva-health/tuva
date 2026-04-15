{{ config(
     enabled = var('semantic_layer_enabled', False) and var('claims_enabled', False)
   )
}}

SELECT
    rsm.collection_end_date
  , rsm.person_id
  , rsm.normalized_risk_score
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('cms_hcc__patient_risk_scores_monthly') }} as rsm