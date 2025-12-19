{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

select distinct
    tccl.person_id
  , csk.condition_sk
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from {{ ref('semantic_layer__stg_chronic_conditions__tuva_chronic_conditions_long') }} as tccl
inner join {{ ref('semantic_layer__dim_condition') }} as csk on tccl.condition = csk.condition