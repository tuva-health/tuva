{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

{%- set tuva_columns -%}
      medication_id
    , person_id
    , patient_id
    , encounter_id
    , dispensing_date
    , prescribing_date
    , source_code_type
    , source_code
    , source_description
    , ndc_code
    , rxnorm_code
    , atc_code
    , route
    , strength
    , quantity
    , quantity_unit
    , days_supply
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , rxnorm_code as x_temp_rxnorm_code #}
    {# , source_code_type as x_temp_source_code_type #}
    {# , source_code as zzz_temp_source_code #}
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
-- depends_on: {{ ref('the_tuva_project', 'synthetic_data__medication') }}
{% endif %}
from {{ source('source_input', 'medication') }}
