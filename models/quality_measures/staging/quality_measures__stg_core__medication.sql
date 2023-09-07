{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medication') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , dispensing_date
    , source_code_type
    , source_code
    , ndc_code
    , rxnorm_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__medication') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      cast(null as {{ dbt.type_string() }} ) as patient_id
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as dispensing_date
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as ndc_code
    , cast(null as {{ dbt.type_string() }} ) as rxnorm_code
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0

{%- endif %}