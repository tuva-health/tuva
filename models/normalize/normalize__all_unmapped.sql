{{ config(
     enabled = ( ( var('enable_normalize_engine', False) == True  or  var('enable_normalize_engine', False) == "unmapped") and
                   var('claims_enabled', var('clinical_enabled', var('tuva_marts_enabled', False)))
               ) | as_bool
   )
}}
with agg_cte as (
    {% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}
    select * From {{ref('normalize__stg_unmapped_lab_result')}} union all
    select * From {{ref('normalize__stg_unmapped_medication')}} union all
    select * From {{ref('normalize__stg_unmapped_observation')}} union all
    {%- endif %}
    select * From {{ref('normalize__stg_unmapped_condition')}} union all
    select * From {{ref('normalize__stg_unmapped_procedure')}}
)

select i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION,
       sum(i.item_count) as item_count
       , {{ dbt.listagg(measure="distinct i.domain", delimiter_text="', '", order_by_clause="order by i.domain" )}} as domains -- noqa
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
from agg_cte i
group by i.SOURCE_CODE_TYPE, i.SOURCE_CODE, i.SOURCE_DESCRIPTION