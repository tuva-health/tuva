{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id 
    , payer 
    , claim_line_number
    , prescribing_provider_id
    , dispensing_date
    , ndc_code
    , ndc_description
    , quantity
    , days_supply
    , refills
    , paid_amount
    , allowed_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__pharmacy_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , claim_id 
    , payer 
    , claim_line_number
    , prescribing_provider_id
    , dispensing_date
    , ndc_code
    , ndc_description
    , quantity
    , days_supply
    , refills
    , paid_amount
    , allowed_amount
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__pharmacy_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as claim_line_number
    , cast(null as {{ dbt.type_string() }} ) as prescribing_provider_id
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_string() }} ) as ndc_description
    , cast(null as {{ dbt.type_string() }} ) as quantity
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_string() }} ) as refills
    , cast(null as {{ dbt.type_string() }} ) as paid_amount
    , cast(null as {{ dbt.type_string() }} ) as allowed_amount
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
    select
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , cast(null as {{ dbt.type_string() }} ) as payer
    , cast(null as {{ dbt.type_string() }} ) as claim_line_number
    , cast(null as {{ dbt.type_string() }} ) as prescribing_provider_id
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_string() }} ) as ndc_description
    , cast(null as {{ dbt.type_string() }} ) as quantity
    , cast(null as {{ dbt.type_int() }} ) as days_supply
    , cast(null as {{ dbt.type_string() }} ) as refills
    , cast(null as {{ dbt.type_string() }} ) as paid_amount
    , cast(null as {{ dbt.type_string() }} ) as allowed_amount
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    limit 0
{%- endif %}

{%- endif %}