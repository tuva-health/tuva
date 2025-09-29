{{ config(
     enabled = var('semantic_layer_enabled',False) | as_bool
   )
}}

with cte as (
select distinct
    service_category_1
    ,service_category_2
    ,service_category_3
from {{ ref('service_category__service_categories') }}
)
select 
    *
    ,ROW_NUMBER() OVER (order by service_category_3) as service_category_sk
from cte