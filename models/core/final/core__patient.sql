{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{%- set age_expression -%}
cast(floor({{ datediff('birth_date', 'tuva_last_run_date', 'hour') }} / 8760.0) as {{ dbt.type_int() }})
{%- endset -%}

{%- set age_group_expression -%}
cast(
    case
        when {{ age_expression }} < 10 then '0-9'
        when {{ age_expression }} < 20 then '10-19'
        when {{ age_expression }} < 30 then '20-29'
        when {{ age_expression }} < 40 then '30-39'
        when {{ age_expression }} < 50 then '40-49'
        when {{ age_expression }} < 60 then '50-59'
        when {{ age_expression }} < 70 then '60-69'
        when {{ age_expression }} < 80 then '70-79'
        when {{ age_expression }} < 90 then '80-89'
        else '90+'
    end as {{ dbt.type_string() }}
)
{%- endset -%}

{%- set tuva_core_columns -%}
      person_id
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

{%- set staged_extension_columns -%}
    {{ select_extension_columns(ref('core__stg_patient'), strip_prefix=false) }}
{%- endset -%}

{%- set final_extension_columns -%}
    {{ select_extension_columns(ref('core__stg_patient'), alias='patient_base', strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , data_source
    , tuva_last_run
{%- endset -%}

with patient_base as (
    select
          person_id
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
        , data_source
        , tuva_last_run
        {{ staged_extension_columns }}
        , cast(substring(cast(tuva_last_run as {{ dbt.type_string() }}), 1, 10) as date) as tuva_last_run_date
    from {{ ref('core__stg_patient') }}
)

select
    {{ tuva_core_columns }}
    , {{ age_expression }} as age
    , {{ age_group_expression }} as age_group
    {{ final_extension_columns }}
    {{ tuva_metadata_columns }}
from patient_base
