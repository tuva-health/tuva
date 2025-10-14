{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT distinct
    year
FROM {{ ref('semantic_layer__dim_date') }} cal