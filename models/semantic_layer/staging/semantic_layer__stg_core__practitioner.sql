{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}


SELECT
    p.npi
  , p.specialty
  , p.practitioner_id
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('core__practitioner') }} as p