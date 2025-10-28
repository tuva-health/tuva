{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

SELECT
      tccl.person_id
    , tccl.condition
    , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('chronic_conditions__tuva_chronic_conditions_long')}} as tccl