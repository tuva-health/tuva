{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
 cast(null as {{ dbt.type_string() }} ) as practitioner_id
, cast(null as {{ dbt.type_string() }} ) as npi
, cast(null as {{ dbt.type_string() }} ) as first_name
, cast(null as {{ dbt.type_string() }} ) as last_name
, cast(null as {{ dbt.type_string() }} ) as practice_affiliation
, cast(null as {{ dbt.type_string() }} ) as specialty
, cast(null as {{ dbt.type_string() }} ) as sub_specialty
, cast(null as {{ dbt.type_string() }} ) as data_source
, cast(null as {{ dbt.type_string() }} ) as file_name
, cast(null as {{ dbt.type_timestamp() }} ) as ingest_datetime
, cast(null as {{ dbt.type_timestamp() }} ) as tuva_last_run
{{ limit_zero()}}

{%- else -%}

select * from {{ source('source_input', 'practitioner') }}

{%- endif %}