{{ config(
     enabled = var('clinical_enabled', false)
 | as_bool
   )
}}

{%- set tuva_columns -%}
      person_id
    , patient_id
    , first_name
    , middle_name
    , last_name
    , name_suffix
    , sex
    , race
    , ethnicity
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
{%- endset -%}

{%- set tuva_extensions -%}
    , {{ dbt.concat([
        "'clinical_'",
        "cast(person_id as " ~ dbt.type_string() ~ ")"
    ]) }} as x_temp_record_origin
{%- endset -%}

{%- set tuva_metadata -%}
    , ingest_datetime
    , data_source
{%- endset -%}

select
    {{ tuva_columns }}
    {{ tuva_extensions }}
    {{ tuva_metadata }}
from {{ ref('the_tuva_project', 'synthetic_data__patient') }}
