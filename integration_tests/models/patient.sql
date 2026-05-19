{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , patient_id
    , first_name
    , middle_name
    , last_name
    , name_suffix
    , sex
    , race
    , ethnicity
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    , {{ dbt.concat([
        "'clinical_'",
        "cast(person_id as " ~ dbt.type_string() ~ ")"
    ]) }} as x_temp_record_origin
    {# , first_name as x_temp_first_name #}
    {# , last_name as zzz_temp_last_name #}
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('the_tuva_project', 'synthetic_data__patient') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'patient') }}

{%- endif %}
