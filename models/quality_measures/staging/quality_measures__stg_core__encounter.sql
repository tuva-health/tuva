{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled',False)) == true and var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , encounter_id
    , encounter_type
    , encounter_group
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__encounter') }}

{% elif var('clinical_enabled', var('tuva_marts_enabled',False)) == true -%}

select
      person_id
    , encounter_id
    , encounter_type
    , encounter_group
    , length_of_stay
    , encounter_start_date
    , encounter_end_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__encounter') }}

{% elif var('claims_enabled', var('tuva_marts_enabled',False)) == true -%}

{% if target.type == 'fabric' %}
    select top 0
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_type
    , cast(null as {{ dbt.type_string() }} ) as encounter_group
    , cast(null as {{ dbt.type_numeric() }} ) as length_of_stay
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_end_date
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{% else %}
    select
      cast(null as {{ dbt.type_string() }} ) as person_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_id
    , cast(null as {{ dbt.type_string() }} ) as encounter_type
    , cast(null as {{ dbt.type_string() }} ) as encounter_group
    , cast(null as {{ dbt.type_numeric() }} ) as length_of_stay
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_start_date
    , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_end_date
    , cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
    limit 0
{%- endif %}

{%- endif %}
