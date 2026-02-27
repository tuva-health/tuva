{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      condition_id
    , payer
    , person_id
    , patient_id
    , encounter_id
    , claim_id
    , recorded_date
    , onset_date
    , resolved_date
    , status
    , condition_type
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , condition_rank
    , present_on_admit_code
    , present_on_admit_description
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , status as x_temp_status #}
    {# , condition_type as x_temp_condition_type #}
    {# , source_code as x_temp_source_code #}
    {# , recorded_date as zzz_temp_recorded_date #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_status #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_condition_type #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_source_code #}
    {# , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as zzz_temp_recorded_date #}
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
  cast(null as {{ dbt.type_string() }}) as condition_id
, cast(null as {{ dbt.type_string() }}) as payer
, cast(null as {{ dbt.type_string() }}) as person_id
, cast(null as {{ dbt.type_string() }}) as patient_id
, cast(null as {{ dbt.type_string() }}) as encounter_id
, cast(null as {{ dbt.type_string() }}) as claim_id
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as recorded_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as onset_date
, {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as resolved_date
, cast(null as {{ dbt.type_string() }}) as status
, cast(null as {{ dbt.type_string() }}) as condition_type
, cast(null as {{ dbt.type_string() }}) as source_code_type
, cast(null as {{ dbt.type_string() }}) as source_code
, cast(null as {{ dbt.type_string() }}) as source_description
, cast(null as {{ dbt.type_string() }}) as normalized_code_type
, cast(null as {{ dbt.type_string() }}) as normalized_code
, cast(null as {{ dbt.type_string() }}) as normalized_description
, cast(null as {{ dbt.type_int() }}) as condition_rank
, cast(null as {{ dbt.type_string() }}) as present_on_admit_code
, cast(null as {{ dbt.type_string() }}) as present_on_admit_description
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
from {{ source('source_input', 'condition') }}

{%- endif %}
