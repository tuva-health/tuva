{{ config(
     enabled = var('clinical_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      person_id
    , patient_id
    , name_suffix
    , first_name
    , middle_name
    , last_name
    , sex
    , race
    , birth_date
    , death_date
    , death_flag
    , social_security_number
    , address
    , city
    , state
    , zip_code
    , county
    , latitude
    , longitude
    , phone
    , email
    , ethnicity
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__stg_patient'), strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , file_name
    , ingest_datetime
    , tuva_last_run
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized_input__stg_patient') }}
