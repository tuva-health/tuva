{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('claims_enabled', var('tuva_marts_enabled',False)) == true and var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      medical_claim_id
    , person_id
    , claim_id
    , claim_line_number
    , claim_type
    , encounter_group
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , payer
    , plan
    , billing_id
    , billing_name
    , admission_date
    , discharge_date
    , bill_type_code
    , revenue_center_code
    , revenue_center_description
    , place_of_service_code
    , place_of_service_description
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , paid_date
    , paid_amount
    , in_network_flag
from {{ ref('core__medical_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      medical_claim_id
    , person_id
    , claim_id
    , claim_line_number
    , claim_type
    , encounter_group
    , claim_start_date
    , claim_end_date
    , claim_line_start_date
    , payer
    , billing_id
    , billing_name
    , admission_date
    , discharge_date
    , bill_type_code
    , revenue_center_code
    , revenue_center_description
    , place_of_service_code
    , place_of_service_description
    , hcpcs_code
    , hcpcs_modifier_1
    , hcpcs_modifier_2
    , hcpcs_modifier_3
    , hcpcs_modifier_4
    , hcpcs_modifier_5
    , paid_date
    , paid_amount
    , in_network_flag
from {{ ref('core__medical_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as medical_claim_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as claim_type
        , cast(null as {{ dbt.type_string() }} ) as encounter_group
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_line_start_date
        , cast(null as {{ dbt.type_string() }} ) as payer
        , cast(null as {{ dbt.type_string() }} ) as billing_id
        , cast(null as {{ dbt.type_string() }} ) as billing_name
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as admission_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as discharge_date
        , cast(null as {{ dbt.type_string() }} ) as bill_type_code
        , cast(null as {{ dbt.type_string() }} ) as revenue_center_code
        , cast(null as {{ dbt.type_string() }} ) as revenue_center_description
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_code
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_description
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_5
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as in_network_flag
{% else %}
    select
          cast(null as {{ dbt.type_string() }} ) as medical_claim_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as claim_type
        , cast(null as {{ dbt.type_string() }} ) as encounter_group
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_line_start_date
        , cast(null as {{ dbt.type_string() }} ) as payer
        , cast(null as {{ dbt.type_string() }} ) as billing_id
        , cast(null as {{ dbt.type_string() }} ) as billing_name
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as admission_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as discharge_date
        , cast(null as {{ dbt.type_string() }} ) as bill_type_code
        , cast(null as {{ dbt.type_string() }} ) as revenue_center_code
        , cast(null as {{ dbt.type_string() }} ) as revenue_center_description
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_code
        , cast(null as {{ dbt.type_string() }} ) as place_of_service_description
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_1
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_2
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_3
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_4
        , cast(null as {{ dbt.type_string() }} ) as hcpcs_modifier_5
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as in_network_flag
    limit 0
{%- endif %}

{%- endif %}