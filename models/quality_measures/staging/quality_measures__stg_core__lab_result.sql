{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

with lab_order as (

    select
          person_id
        , result
        , result_datetime as result_date
        , collection_datetime as collection_date
        , lower(coalesce(normalized_order_type, source_order_type)) as code_type
        , coalesce(normalized_order_code, source_order_code) as code
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is null

)

, lab_component as (

    select
          person_id
        , result
        , result_datetime as result_date
        , collection_datetime as collection_date
        , lower(coalesce(normalized_component_type, source_component_type)) as code_type
        , coalesce(normalized_component_code, source_component_code) as code
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is not null

)

, unioned as (

    select * from lab_order
    union all
    select * from lab_component

)

select distinct
      person_id
    , result
    , result_date
    , collection_date
    , code_type
    , code
from unioned

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

with lab_order as (

    select
          person_id
        , result
        , result_datetime as result_date
        , collection_datetime as collection_date
        , lower(coalesce(normalized_order_type, source_order_type)) as code_type
        , coalesce(normalized_order_code, source_order_code) as code
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is null

)

, lab_component as (

    select
          person_id
        , result
        , result_datetime as result_date
        , collection_datetime as collection_date
        , lower(coalesce(normalized_component_type, source_component_type)) as code_type
        , coalesce(normalized_component_code, source_component_code) as code
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is not null

)

, unioned as (

    select * from lab_order
    union all
    select * from lab_component

)

select distinct
      person_id
    , result
    , result_date
    , collection_date
    , code_type
    , code
from unioned

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as result
    , {{ try_to_cast_datetime('null') }} as result_date
    , {{ try_to_cast_datetime('null') }} as collection_date
    , cast(null as {{ dbt.type_string() }} ) as code_type
    , cast(null as {{ dbt.type_string() }} ) as code
{{ limit_zero()}}

{%- endif %}
