{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      observation_id
    , person_id
    , encounter_id
    , observation_type
    , normalized_code_type
    , normalized_code
    , normalized_description
    , source_code_type
    , source_code
    , source_description
    , observation_date
    , result
    , normalized_units
    , source_units
    , data_source
from {{ ref('core__observation') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      observation_id
    , person_id
    , encounter_id
    , observation_type
    , normalized_code_type
    , normalized_code
    , normalized_description
    , source_code_type
    , source_code
    , source_description
    , observation_date
    , result
    , normalized_units
    , source_units
    , data_source
from {{ ref('core__observation') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as observation_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , cast(null as {{ dbt.type_string() }} ) as observation_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_description
        , cast(null as {{ dbt.type_string() }} ) as source_code_type
        , cast(null as {{ dbt.type_string() }} ) as source_code
        , cast(null as {{ dbt.type_string() }} ) as source_description
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as observation_date
        , cast(null as {{ dbt.type_string() }} ) as result
        , cast(null as {{ dbt.type_string() }} ) as normalized_units
        , cast(null as {{ dbt.type_string() }} ) as source_units
        , cast(null as {{ dbt.type_string() }} ) as data_source
{% else %}
    select
          cast(null as {{ dbt.type_string() }} ) as observation_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , cast(null as {{ dbt.type_string() }} ) as observation_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_description
        , cast(null as {{ dbt.type_string() }} ) as source_code_type
        , cast(null as {{ dbt.type_string() }} ) as source_code
        , cast(null as {{ dbt.type_string() }} ) as source_description
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as observation_date
        , cast(null as {{ dbt.type_string() }} ) as result
        , cast(null as {{ dbt.type_string() }} ) as normalized_units
        , cast(null as {{ dbt.type_string() }} ) as source_units
        , cast(null as {{ dbt.type_string() }} ) as data_source
    limit 0
{%- endif %}

{%- endif %}