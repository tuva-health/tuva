{{ config(
     enabled = var('clinical_enabled', False)
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

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ tuva_source('condition') }}
