{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
cast(null as {{ dbt.type_string() }} ) as appointment_id
, cast(null as {{ dbt.type_string() }} ) as patient_id
, cast(null as {{ dbt.type_string() }} ) as provider_id
, cast(null as {{ dbt.type_string() }} ) as status
, cast(null as {{ dbt.type_string() }} ) as cancellation_reason
, cast(null as {{ dbt.type_string() }} ) as appointment_type_code
, cast(null as {{ dbt.type_string() }} ) as reason_code
, cast(null as {{ dbt.type_string() }} ) as reason_description
, cast(NULL AS {{ dbt.type_timestamp() }}) AS start_time
, cast(NULL AS {{ dbt.type_timestamp() }}) AS end_time
, cast(null as {{ dbt.type_timestamp() }} ) as created_at
, cast(null as {{ dbt.type_timestamp() }} ) as updated_at
, cast(null as {{ dbt.type_string() }} ) as location_id
, cast(null as {{ dbt.type_string() }} ) as data_source
, cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{{ limit_zero()}}

{%- else -%}

select * from {{ source('source_input', 'appointment') }}

{%- endif %}
