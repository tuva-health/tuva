{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with cte as (
  SELECT DISTINCT
      condition
  FROM {{ ref('semantic_layer__stg_chronic_conditions__tuva_chronic_conditions_long')}}
)

select 
    {{ dbt_utils.generate_surrogate_key(['condition']) }} as condition_sk
  , condition
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte