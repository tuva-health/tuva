{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
cast(null as {{ dbt.type_string() }}) as encounter_id
, cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as encounter_type
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_start_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as encounter_end_date
, cast(null as {{ dbt.type_int() }}) as length_of_stay
, cast(null as {{ dbt.type_string() }}) as admit_source_code
, cast(null as {{ dbt.type_string() }}) as admit_source_description
, cast(null as {{ dbt.type_string() }}) as admit_type_code
, cast(null as {{ dbt.type_string() }}) as admit_type_description
, cast(null as {{ dbt.type_string() }}) as discharge_disposition_code
, cast(null as {{ dbt.type_string() }}) as discharge_disposition_description
, cast(null as {{ dbt.type_string() }}) as attending_provider_id
, cast(null as {{ dbt.type_string() }}) as attending_provider_name
, cast(null as {{ dbt.type_string() }}) as facility_id
, cast(null as {{ dbt.type_string() }}) as facility_name
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code_type
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_code
, cast(null as {{ dbt.type_string() }}) as primary_diagnosis_description
, cast(null as {{ dbt.type_string() }}) as drg_code_type
, cast(null as {{ dbt.type_string() }}) as drg_code
, cast(null as {{ dbt.type_string() }}) as drg_description
, cast(null as {{ dbt.type_float() }}) as paid_amount
, cast(null as {{ dbt.type_float() }}) as allowed_amount
, cast(null as {{ dbt.type_float() }}) as charge_amount
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
{{ limit_zero() }}

{%- else -%}

select * from {{ source('source_input', 'encounter') }}

{%- endif %}
