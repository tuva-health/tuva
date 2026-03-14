{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with cte as (
  select distinct
      service_category_1
    , service_category_2
    , service_category_3
  from {{ ref('service_category__service_categories') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['service_category_1', 'service_category_2', 'service_category_3']) }} as service_category_sk
  , service_category_1
  , service_category_2
  , service_category_3
  , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from cte