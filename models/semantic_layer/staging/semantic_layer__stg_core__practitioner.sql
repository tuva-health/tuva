{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


SELECT
    p.npi
  , p.specialty
  , p.practitioner_id
FROM {{ ref('core__practitioner') }} as p