{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      practitioner_id
    , npi
    , first_name
    , last_name
    , practice_affiliation
    , specialty
    , sub_specialty
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , specialty as x_temp_specialty #}
    {# , first_name as x_temp_first_name #}
    {# , last_name as x_temp_last_name #}
    {# , practice_affiliation as zzz_temp_practice_affiliation #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{# Uncomment the synthetic extension columns below to test extension columns passthrough feature #}
{%- set tuva_synthetic_extensions -%}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_specialty #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_first_name #}
    {# , cast(null as {{ dbt.type_string() }}) as x_temp_last_name #}
    {# , cast(null as {{ dbt.type_string() }}) as zzz_temp_practice_affiliation #}
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select {% if target.type == 'fabric' %} top 0 {% else %}{% endif %}
  cast(null as {{ dbt.type_string() }}) as practitioner_id
, cast(null as {{ dbt.type_string() }}) as npi
, cast(null as {{ dbt.type_string() }}) as first_name
, cast(null as {{ dbt.type_string() }}) as last_name
, cast(null as {{ dbt.type_string() }}) as practice_affiliation
, cast(null as {{ dbt.type_string() }}) as specialty
, cast(null as {{ dbt.type_string() }}) as sub_specialty
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
from {{ source('source_input', 'practitioner') }}

{%- endif %}
