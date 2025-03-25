{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , dispensing_date
    , ndc_code
    , paid_date
    , data_source
from {{ ref('core__pharmacy_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , dispensing_date
    , ndc_code
    , paid_date
    , data_source
from {{ ref('core__pharmacy_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as person_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
        , cast(null as {{ dbt.type_string() }} ) as ndc_code
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as data_source
{% else %}
    select
          cast(null as {{ dbt.type_string() }} ) as person_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
        , cast(null as {{ dbt.type_string() }} ) as ndc_code
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as data_source
    limit 0
{%- endif %}

{%- endif %}
