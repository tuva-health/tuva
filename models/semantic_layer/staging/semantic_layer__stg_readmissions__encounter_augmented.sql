{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
  ea.*
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('readmissions__encounter_augmented') }} as ea 