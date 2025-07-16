{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('clinical_enabled', var('tuva_marts_enabled', False))
               ) | as_bool
   )
}}

{% if var('enable_normalize_engine',false) == True %}

select
      i.source_component_type as source_code_type
    , i.source_component_code as source_code
    , i.source_component_description as source_description
    , count(*) as item_count
    , 'lab_result' as domain
    , i.data_source
from {{ref('core__lab_result')}} i
left join {{ ref('custom_mapped') }} custom_mapped
    on  ( lower(i.source_component_type) = lower(custom_mapped.source_code_type)
        or ( i.source_component_type is null and custom_mapped.source_code_type is null)
        )
    and (i.source_component_code = custom_mapped.source_code
        or ( i.source_component_code is null and custom_mapped.source_code is null)
        )
    and (i.source_component_description = custom_mapped.source_description
        or ( i.source_component_description is null and custom_mapped.source_description is null)
        )
where i.normalized_component_code is null and i.normalized_component_description is null
    and not ( i.source_component_code is null and i.source_component_description is null)
    and custom_mapped.not_mapped is null
group by
      i.source_component_type
    , i.source_component_code
    , i.source_component_description
    , i.data_source

{% else %}

select
      i.source_component_type as source_code_type
    , i.source_component_code as source_code
    , i.source_component_description as source_description
    , count(*) as item_count
    , 'lab_result' as domain
    , i.data_source
from {{ref('core__lab_result')}} i
where i.normalized_component_code is null and i.normalized_component_description is null
    and not ( i.source_component_code is null and i.source_component_description is null)
group by
      i.source_component_type
    , i.source_component_code
    , i.source_component_description
    , i.data_source

{% endif %}