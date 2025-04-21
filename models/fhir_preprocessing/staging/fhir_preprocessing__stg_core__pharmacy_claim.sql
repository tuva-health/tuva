{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('claims_enabled', var('tuva_marts_enabled',False)) == true and var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      pharmacy_claim_id
    , person_id
    , claim_id
    , claim_line_number
    , payer
    , plan
    , dispensing_provider_id
    , dispensing_provider_name
    , dispensing_date
    , paid_date
    , days_supply
    , refills
    , paid_amount
    , in_network_flag
    , ndc_code
from {{ ref('core__pharmacy_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      pharmacy_claim_id
    , person_id
    , claim_id
    , claim_line_number
    , payer
    , dispensing_provider_id
    , dispensing_provider_name
    , dispensing_date
    , paid_date
    , days_supply
    , refills
    , paid_amount
    , in_network_flag
    , ndc_code
from {{ ref('core__pharmacy_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
          cast(null as {{ dbt.type_string() }} ) as pharmacy_claim_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as payer
        , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_id
        , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_name
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as days_supply
        , cast(null as {{ dbt.type_string() }} ) as refills
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as in_network_flag
        , cast(null as {{ dbt.type_string() }} ) as ndc_code
{% else %}
    select
          cast(null as {{ dbt.type_string() }} ) as pharmacy_claim_id
        , cast(null as {{ dbt.type_string() }} ) as person_id
        , cast(null as {{ dbt.type_string() }} ) as claim_id
        , cast(null as {{ dbt.type_string() }} ) as claim_line_number
        , cast(null as {{ dbt.type_string() }} ) as payer
        , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_id
        , cast(null as {{ dbt.type_string() }} ) as dispensing_provider_name
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as paid_date
        , cast(null as {{ dbt.type_string() }} ) as days_supply
        , cast(null as {{ dbt.type_string() }} ) as refills
        , cast(null as {{ dbt.type_string() }} ) as paid_amount
        , cast(null as {{ dbt.type_string() }} ) as in_network_flag
        , cast(null as {{ dbt.type_string() }} ) as ndc_code
    limit 0
{%- endif %}

{%- endif %}