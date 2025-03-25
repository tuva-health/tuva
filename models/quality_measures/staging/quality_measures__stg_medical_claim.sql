{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
         cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_5
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
select
         cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_5
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    limit 0
{%- endif %}

{%- endif %}
