{{ config(
     enabled = var('semantic_layer_enabled', False) and var('claims_enabled', False)
   )
}}

SELECT
  ea.*
FROM {{ ref('readmissions__encounter_augmented') }} as ea