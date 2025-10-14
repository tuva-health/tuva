{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT DISTINCT
    year
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('semantic_layer__dim_date') }} cal