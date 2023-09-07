{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__encounter') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('core__encounter') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      cast(null as {{ dbt.type_string() }} ) as patient_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_type
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_end_date
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
limit 0

{%- endif %}