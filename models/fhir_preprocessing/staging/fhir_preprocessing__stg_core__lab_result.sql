{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

with lab_order as (

    select
          lab_result_id
        , person_id
        , encounter_id
        , status
        , lower(coalesce(normalized_order_type, source_order_type)) as code_type
        , coalesce(normalized_order_code, source_order_code) as code
        , coalesce(normalized_order_description, source_order_description) as description
        , result_date
        , result
        , coalesce(normalized_units, source_units) as units
        , data_source
    from {{ ref('core__lab_result') }}

)

, lab_component as (

    select
          lab_result_id
        , person_id
        , encounter_id
        , status
        , lower(coalesce(normalized_component_type, source_component_type)) as code_type
        , coalesce(normalized_component_code, source_component_code) as code
        , coalesce(normalized_component_description, source_component_description) as description
        , result_date
        , result
        , coalesce(normalized_units, source_units) as units
        , data_source
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is not null

)

, unioned as (

    select * from lab_order
    union all
    select * from lab_component

)

select distinct
      lab_result_id
    , person_id
    , encounter_id
    , status
    , code_type
    , code
    , description
    , result_date
    , result
    , units
    , data_source
from unioned

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

with lab_order as (

    select
          lab_result_id
        , person_id
        , encounter_id
        , status
        , lower(coalesce(normalized_order_type, source_order_type)) as code_type
        , coalesce(normalized_order_code, source_order_code) as code
        , coalesce(normalized_order_description, source_order_description) as description
        , result_date
        , result
        , coalesce(normalized_units, source_units) as units
        , data_source
    from {{ ref('core__lab_result') }}

)

, lab_component as (

    select
          lab_result_id
        , person_id
        , encounter_id
        , status
        , lower(coalesce(normalized_component_type, source_component_type)) as code_type
        , coalesce(normalized_component_code, source_component_code) as code
        , coalesce(normalized_component_description, source_component_description) as description
        , result_date
        , result
        , coalesce(normalized_units, source_units) as units
        , data_source
    from {{ ref('core__lab_result') }}
    where coalesce(normalized_component_code, source_component_code) is not null

)

, unioned as (

    select * from lab_order
    union all
    select * from lab_component

)

select distinct
      lab_result_id
    , person_id
    , encounter_id
    , status
    , code_type
    , code
    , description
    , result_date
    , result
    , units
    , data_source
from unioned

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
      cast(null as {{ dbt.type_string() }} ) as lab_result_id
    , cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as status
    , cast(null as {{ dbt.type_string() }} ) as code_type
    , cast(null as {{ dbt.type_string() }} ) as code
    , cast(null as {{ dbt.type_string() }} ) as description
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as result_date
    , cast(null as {{ dbt.type_string() }} ) as result
    , cast(null as {{ dbt.type_string() }} ) as units
    , cast(null as {{ dbt.type_string() }} ) as data_source
{{ limit_zero()}}

{%- endif %}
