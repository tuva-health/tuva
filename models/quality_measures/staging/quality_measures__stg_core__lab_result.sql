{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , result_date
    , collection_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__lab_result') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , result_date
    , collection_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__lab_result') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      cast(null as {{ dbt.type_string() }} ) as patient_id
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as result_date
    ,  {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as collection_date
    , cast(null as {{ dbt.type_string() }} ) as source_code_type
    , cast(null as {{ dbt.type_string() }} ) as source_code
    , cast(null as {{ dbt.type_string() }} ) as normalized_code_type
    , cast(null as {{ dbt.type_string() }} ) as normalized_code
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0

{%- endif %}