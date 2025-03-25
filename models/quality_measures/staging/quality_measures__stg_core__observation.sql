{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , encounter_id
    , observation_date
    , result
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , normalized_description
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__observation') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , encounter_id
    , observation_date
    , result
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , normalized_description
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__observation') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as observation_date
        , cast(null as {{ dbt.type_string() }} ) as result
        , cast(null as {{ dbt.type_string() }} ) as source_code_type
        , cast(null as {{ dbt.type_string() }} ) as source_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_description
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
select
          cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as observation_date
        , cast(null as {{ dbt.type_string() }} ) as result
        , cast(null as {{ dbt.type_string() }} ) as source_code_type
        , cast(null as {{ dbt.type_string() }} ) as source_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
        , cast(null as {{ dbt.type_string() }} ) as normalized_code
        , cast(null as {{ dbt.type_string() }} ) as normalized_description
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    limit 0
{%- endif %}

{%- endif %}
