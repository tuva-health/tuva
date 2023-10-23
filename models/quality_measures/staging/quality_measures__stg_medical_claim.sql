{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , claim_id
    , claim_start_date
    , claim_end_date
    , place_of_service_code
    , hcpcs_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medical_claim') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
     cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as claim_id
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_start_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claim_end_date
    , cast(null as {{ dbt.type_string() }} ) as place_of_service_code
    , cast(null as {{ dbt.type_string() }} ) as hcpcs_code
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0

{%- endif %}