{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

{%- set tuva_columns -%}
      immunization_id
    , person_id
    , patient_id
    , encounter_id
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , status
    , status_reason
    , occurrence_date
    , source_dose
    , normalized_dose
    , lot_number
    , body_site
    , route
    , location_id
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , person_id as x_temp_person_id #}
    {# , source_code as x_temp_source_code #}
    {# , source_code_type as zzz_temp_source_code_type #}
{%- endset -%}

{%- set tuva_metadata -%}
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

{% if var('use_synthetic_data') == true -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('immunization_seed') }}

{%- else -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ source('source_input', 'immunization') }}

{%- endif %}
