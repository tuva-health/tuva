{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
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
    , ndc_description
    , rxnorm_code
    , rxnorm_description
    , atc_code
    , atc_description
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
    , data_source
    , file_name
    , ingest_datetime
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ tuva_source('medication') }}
