{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with cte as (
SELECT DISTINCT
    condition
  , '{{ var('tuva_last_run') }}' as tuva_last_run
FROM {{ ref('chronic_conditions__tuva_chronic_conditions_long')}}
)
select 
    condition
  , ROW_NUMBER() OVER (order by condition) as condition_sk
from cte