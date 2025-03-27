{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('clinical_enabled', var('tuva_marts_enabled', False))
               ) | as_bool
   )
}}
select i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION,
       sum(i.item_count) as item_count,
       i.domain as domain
       , {{ dbt.listagg(measure="distinct i.DATA_SOURCE", delimiter_text="', '", order_by_clause="order by i.DATA_SOURCE" )}} as data_sources -- noqa
       , cast( null as {{ dbt.type_string() }} ) as normalized_code_type,
       cast( null as {{ dbt.type_string() }} ) as normalized_code,
       cast( null as {{ dbt.type_string() }} ) as normalized_description,
       cast( null as {{ dbt.type_string() }} ) as not_mapped,
       cast( null as {{ dbt.type_string() }} ) as added_by,
       cast( null as {{ dbt.type_string() }} ) as added_date,
       cast( null as {{ dbt.type_string() }} ) as reviewed_by,
       cast( null as {{ dbt.type_string() }} ) as reviewed_date,
       cast( null as {{ dbt.type_string() }} ) as notes
from {{ref('normalize__stg_unmapped_medication')}} i
group by i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION, i.DOMAIN