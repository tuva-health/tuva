{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_columns -%}
      source_condition_id
    , person_id
    , patient_id
    , encounter_id
    , recorded_date
    , onset_date
    , resolved_date
    , status
    , condition_type
    , code_system
    , source_code
    , source_description
    , condition_rank
    , present_on_admit_code
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , status as x_temp_status #}
    {# , condition_type as x_temp_condition_type #}
    {# , source_code as x_temp_source_code #}
    {# , recorded_date as zzz_temp_recorded_date #}
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
{% if the_tuva_project.tuva_synthetic_data_enabled() %}
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__condition') }}
{% endif %}
from {{ source('source_input', 'condition') }}
