{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_columns -%}
      encounter_id
    , person_id
    , patient_id
    , encounter_type
    , encounter_start_date
    , encounter_end_date
    , admit_source_code
    , admit_type_code
    , discharge_disposition_code
    , attending_provider_id
    , attending_provider_name
    , facility_npi
    , facility_name
    , primary_diagnosis_code_type
    , primary_diagnosis_code
    , drg_code_type
    , drg_code
    , paid_amount
    , allowed_amount
    , charge_amount
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , encounter_type as x_temp_encounter_type #}
    {# , encounter_start_date as x_temp_encounter_start_date #}
    {# , facility_name as zzz_temp_facility_name #}
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
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__encounter') }}
{% endif %}
from {{ source('source_input', 'encounter') }}
