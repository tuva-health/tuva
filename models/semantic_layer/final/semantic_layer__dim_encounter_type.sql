{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select
    *
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('encounter_type_sk') }}