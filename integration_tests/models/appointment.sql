{{ config(
     enabled = var('clinical_enabled', false)
 | as_bool
   )
}}

{%- set tuva_columns -%}
      appointment_id
    , person_id
    , patient_id
    , encounter_id
    , start_datetime
    , end_datetime
    , duration
    , location_id
    , practitioner_id
    , type_code
    , type_description
    , status_code
    , status_description
    , reason
    , cancellation_reason
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , type_code as x_temp_type_code #}
    {# , start_datetime as x_temp_start_datetime #}
    {# , reason as zzz_temp_reason #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
{% if the_tuva_project.tuva_synthetic_data_enabled() %}
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__appointment') }}
{% endif %}
from {{ source('source_input', 'appointment') }}
