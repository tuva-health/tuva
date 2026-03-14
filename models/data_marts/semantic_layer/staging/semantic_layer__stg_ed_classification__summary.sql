{{ config(
     enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
   )
}}

SELECT
    s.encounter_id
  , s.person_id
  , s.ed_classification_order
  , s.ed_classification_description
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
FROM {{ ref('ed_classification__summary')}} s