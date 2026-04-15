{{ config(
     enabled = var('semantic_layer_enabled', False) and var('claims_enabled', False)
   )
}}

SELECT DISTINCT
  e.data_source
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('core__eligibility') }} as e