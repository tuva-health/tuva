{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('clinical_enabled', var('tuva_marts_enabled', False))
               ) | as_bool
   )
}}
select
      i.source_order_type
    , i.source_order_code
    , i.source_order_description
    , i.source_component_type
    , i.source_component_code
    , i.source_component_description
    , sum(i.item_count) as item_count
    , i.domain as domain
    , {{ dbt.listagg(measure="distinct i.data_source", delimiter_text="', '", order_by_clause="order by i.data_source" )}} as data_sources -- noqa
    , cast( null as {{ dbt.type_string() }} ) as normalized_order_type
    , cast( null as {{ dbt.type_string() }} ) as normalized_order_code
    , cast( null as {{ dbt.type_string() }} ) as normalized_order_description
    , cast( null as {{ dbt.type_string() }} ) as normalized_component_type
    , cast( null as {{ dbt.type_string() }} ) as normalized_component_code
    , cast( null as {{ dbt.type_string() }} ) as normalized_component_description
    , cast( null as {{ dbt.type_string() }} ) as not_mapped
    , cast( null as {{ dbt.type_string() }} ) as added_by
    , cast( null as {{ dbt.type_string() }} ) as added_date
    , cast( null as {{ dbt.type_string() }} ) as reviewed_by
    , cast( null as {{ dbt.type_string() }} ) as reviewed_date
    , cast( null as {{ dbt.type_string() }} ) as notes
from {{ref('normalize__stg_unmapped_lab_result')}} i
group by
      i.source_order_type
    , i.source_order_code
    , i.source_order_description
    , i.source_component_type
    , i.source_component_code
    , i.source_component_description
    , i.domain