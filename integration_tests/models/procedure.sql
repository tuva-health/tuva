{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_columns -%}
      source_procedure_id
    , person_id
    , patient_id
    , encounter_id
    , procedure_date
    , code_system
    , source_code
    , source_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , source_procedure_id as x_temp_source_procedure_id #}
    {# , person_id as x_temp_person_id #}
    {# , patient_id as zzz_temp_patient_id #}
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
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__procedure') }}
{% endif %}
from {{ source('source_input', 'procedure') }}
