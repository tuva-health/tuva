{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT DISTINCT
  e.data_source
FROM {{ ref('core__eligibility') }} as e