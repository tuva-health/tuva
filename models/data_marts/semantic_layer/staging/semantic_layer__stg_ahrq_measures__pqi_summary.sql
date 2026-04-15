{{ config(
     enabled = var('semantic_layer_enabled', False) and var('claims_enabled', False)
   )
}}


SELECT
    p.pqi_number
  , p.pqi_name
  , p.encounter_id
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('ahrq_measures__pqi_summary') }} as p
