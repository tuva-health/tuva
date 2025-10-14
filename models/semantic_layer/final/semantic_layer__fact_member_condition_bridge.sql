{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select distinct
    tccl.person_id
  , csk.condition_sk
from {{ ref('chronic_conditions__tuva_chronic_conditions_long') }} tccl
inner join {{ ref('semantic_layer__dim_condition') }} csk on tccl.condition = csk.condition