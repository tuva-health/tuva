{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select
    *
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('encounter_type_sk') }}