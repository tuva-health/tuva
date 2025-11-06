{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT DISTINCT
    encounter_id
  , service_category_sk
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('semantic_layer__fact_claims') }}