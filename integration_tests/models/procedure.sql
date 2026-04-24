{{ config(
     enabled = var('clinical_enabled', False)
 | as_bool
   )
}}

{%- set tuva_columns -%}
      procedure_id
    , person_id
    , patient_id
    , encounter_id
    , claim_id
    , procedure_date
    , source_code_type
    , source_code
    , source_description
    , normalized_code_type
    , normalized_code
    , normalized_description
    , modifier_1
    , modifier_2
    , modifier_3
    , modifier_4
    , modifier_5
    , practitioner_id
{%- endset -%}

{# Uncomment the columns below to test extension columns passthrough feature #}
{%- set tuva_extensions -%}
    {# , procedure_id as x_temp_procedure_id #}
    {# , person_id as x_temp_person_id #}
    {# , patient_id as zzz_temp_patient_id #}
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
from {{ tuva_source('procedure') }}
