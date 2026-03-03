{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      procedure_id
    , person_id
    , patient_id
    , encounter_id
    , claim_id
    , procedure_date
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , procedure_id as x_temp_procedure_id #}
    {# , person_id as x_temp_person_id #}
    {# , patient_id as zzz_temp_patient_id #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_procedure_id #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_person_id #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_patient_id #}
{%- endset -%}

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
{{ tuva_synthetic_extensions }}
, cast(null as {{ dbt.type_string() }}) as data_source
, cast(null as {{ dbt.type_string() }}) as file_name
, cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
{{ limit_zero() }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'procedure') }}

{%- endif %}
