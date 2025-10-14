{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select *
from {{ ref('encounter_type_sk') }}