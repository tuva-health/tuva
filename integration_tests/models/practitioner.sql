{{ config(
     enabled = var('clinical_enabled', False)
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

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ tuva_source('practitioner') }}
