{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


SELECT
  p.pqi_number
  , p.pqi_name
FROM {{ ref('ahrq_measures__pqi_summary') }} as p
