{{ config(
     enabled = var('clinical_enabled', False)
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
    , status
    , status_reason
    , occurrence_date
    , source_dose
    , lot_number
    , body_site
    , route
    , location_id
    , practitioner_id
{%- endset -%}

{%- set tuva_extensions -%}
    , 'immunization' as x_tuva_test_extension
    , 'immunization' as ext_tuva_test_extension
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('the_tuva_project', 'synthetic_data__immunization') }}
