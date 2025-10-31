{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
    s.encounter_id
  , s.person_id
  , s.ed_classification_order
  , s.ed_classification_description
FROM {{ ref('ed_classification__summary')}} s