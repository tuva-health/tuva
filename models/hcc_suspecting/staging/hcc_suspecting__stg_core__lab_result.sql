{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      lab_result_id
    , person_id
    , lower(coalesce(normalized_code_type, source_code_type)) as code_type
    , coalesce(normalized_code, source_code) as code
    , status
    , result
    , result_date
    , data_source
from {{ ref('core__lab_result') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      lab_result_id
    , person_id
    , lower(coalesce(normalized_code_type,source_code_type)) as code_type
    , coalesce(normalized_code,source_code) as code
    , status
    , result
    , result_date
    , data_source
from {{ ref('core__lab_result') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as lab_result_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as code_type
        , cast(null as {{ dbt.type_string() }} ) as code
        , cast(null as {{ dbt.type_string() }} ) as status
        , cast(null as {{ dbt.type_string() }} ) as result
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as result_date
        , cast(null as {{ dbt.type_string() }} ) as data_source
{% else %}
    select
          cast(null as {{ dbt.type_string() }} ) as lab_result_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as code_type
        , cast(null as {{ dbt.type_string() }} ) as code
        , cast(null as {{ dbt.type_string() }} ) as status
        , cast(null as {{ dbt.type_string() }} ) as result
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as result_date
        , cast(null as {{ dbt.type_string() }} ) as data_source
    limit 0
{%- endif %}

{%- endif %}
