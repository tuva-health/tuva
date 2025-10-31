{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
  *
FROM {{ ref('readmissions__encounter_augmented') }} as ea 