{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


SELECT
    p.npi
  , p.specialty
FROM {{ ref('core__practitioner') }} as p