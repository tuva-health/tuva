{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT DISTINCT
    encounter_id
  , service_category_sk
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('semantic_layer__fact_claims') }}