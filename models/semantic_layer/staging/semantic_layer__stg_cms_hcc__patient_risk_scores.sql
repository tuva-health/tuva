{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    rs.person_id
  , rs.v24_risk_score
  , rs.v28_risk_score
  , rs.blended_risk_score
  , rs.normalized_risk_score
  , rs.payment_risk_score
  , rs.payment_risk_score_weighted_by_months
  , rs.member_months
  , rs.payment_year
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('cms_hcc__patient_risk_scores') }} as rs