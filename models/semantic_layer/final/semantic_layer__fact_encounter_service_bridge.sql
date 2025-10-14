{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT DISTINCT
    encounter_id
  , service_category_sk
from {{ ref('semantic_layer__fact_claims') }}