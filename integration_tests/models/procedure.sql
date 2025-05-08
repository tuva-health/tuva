{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
 cast(null as {{ dbt.type_string() }}) as procedure_id
, cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as encounter_id
, cast(null as {{ dbt.type_string() }}) as claim_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as procedure_date
, cast(null as {{ dbt.type_string() }}) as source_code_type
, cast(null as {{ dbt.type_string() }}) as source_code
, cast(null as {{ dbt.type_string() }}) as source_description
, cast(null as {{ dbt.type_string() }}) as normalized_code_type
, cast(null as {{ dbt.type_string() }}) as normalized_code
, cast(null as {{ dbt.type_string() }}) as normalized_description
, cast(null as {{ dbt.type_string() }}) as modifier_1
, cast(null as {{ dbt.type_string() }}) as modifier_2
, cast(null as {{ dbt.type_string() }}) as modifier_3
, cast(null as {{ dbt.type_string() }}) as modifier_4
, cast(null as {{ dbt.type_string() }}) as modifier_5
, cast(null as {{ dbt.type_string() }}) as practitioner_id
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
, cast(null as {{ dbt.type_timestamp() }}) as tuva_last_run
{{ limit_zero() }}

{%- else -%}

select * from {{ source('source_input', 'procedure') }}

{%- endif %}
