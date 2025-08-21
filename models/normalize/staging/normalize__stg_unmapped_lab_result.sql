{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('clinical_enabled', var('tuva_marts_enabled', False))
               ) | as_bool
   )
}}

with lab_result as (

    select * from {{ ref('core__lab_result') }}

)

, orders as (

    select distinct
          source_order_type as source_code_type
        , source_order_code as source_code
        , source_order_description as source_description
        , normalized_order_type as normalized_code_type
        , normalized_order_code as normalized_code
        , normalized_order_description as normalized_description
    from lab_result

)

, components as (

    select distinct
          source_component_type as source_code_type
        , source_component_code as source_code
        , source_component_description as source_description
        , normalized_component_type as normalized_code_type
        , normalized_component_code as normalized_code
        , normalized_component_description as normalized_description
    from lab_result

)

, unioned as (

    select * from orders
    union all
    select * from components

)

{% if var('enable_normalize_engine',false) == True %}

select
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , count(*) as item_count
    , 'lab_result' as domain
    , unioned.data_source
from unioned
    left join {{ ref('custom_mapped') }} custom_mapped
        on  (lower(unioned.source_code_type) = lower(custom_mapped.source_code_type)
            or (unioned.source_code_type is null and custom_mapped.source_code_type is null)
        )
        and (unioned.source_code = custom_mapped.source_code
            or (unioned.source_code is null and custom_mapped.source_code is null)
        )
        and (unioned.source_description = custom_mapped.source_description
            or (unioned.source_description is null and custom_mapped.source_description is null)
        )
where unioned.normalized_code is null and unioned.normalized_description is null
    and not (unioned.source_code is null and unioned.source_description is null)
    and custom_mapped.not_mapped is null
group by
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , unioned.data_source

{% else %}

select
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , count(*) as item_count
    , 'lab_result' as domain
    , unioned.data_source
from unioned
where unioned.normalized_code is null and unioned.normalized_description is null
    and not (unioned.source_code is null and unioned.source_description is null)
group by
      unioned.source_code_type
    , unioned.source_code
    , unioned.source_description
    , unioned.data_source

{% endif %}