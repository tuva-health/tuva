{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id
    , encounter_id
    , payer 
    , claim_start_date
    , claim_end_date
    , claim_line_number
    , service_category_1
    , service_category_2
    , service_category_3
    , ms_drg_code
    , apr_drg_code
    , hcpcs_code
    , rendering_id
    , paid_amount
    , allowed_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id
    , encounter_id
    , payer 
    , claim_start_date
    , claim_end_date
    , claim_line_number
    , service_category_1
    , service_category_2
    , service_category_3
    , ms_drg_code
    , apr_drg_code
    , hcpcs_code
    , rendering_id
    , paid_amount
    , allowed_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
         cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , cast(null as {{ dbt.type_string() }} ) as payer
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as service_category_1
        , cast(null as {{ dbt.type_string() }} ) as service_category_2
        , cast(null as {{ dbt.type_string() }} ) as service_category_3
        , cast(null as {{ dbt.type_string() }} ) as ms_drg_code
        , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as rendering_id
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as allowed_amount
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
select
         cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as encounter_id
        , cast(null as {{ dbt.type_string() }} ) as payer
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as service_category_1
        , cast(null as {{ dbt.type_string() }} ) as service_category_2
        , cast(null as {{ dbt.type_string() }} ) as service_category_3
        , cast(null as {{ dbt.type_string() }} ) as ms_drg_code
        , cast(null as {{ dbt.type_string() }} ) as apr_drg_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as rendering_id
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as allowed_amount
        , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    limit 0
{%- endif %}

{%- endif %}