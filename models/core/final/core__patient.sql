{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('claims_enabled', false))
            or (the_tuva_project.tuva_boolean_var('clinical_enabled', false))
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

{%- set final_core_columns -%}
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

{%- set final_metadata_columns -%}
    , ingest_datetime
    , tuva_last_run
    , data_source
{%- endset -%}

{% if the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true
   and the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

{%- if execute -%}
    {%- set passthrough_config = get_extension_passthrough_config() -%}
    {%- set passthrough_prefix = passthrough_config['prefix'] | lower -%}
    {%- set clinical_extension_columns = [] -%}

    {%- for col in adapter.get_columns_in_relation(ref('core__int_patient_remove_duplicates')) -%}
        {%- if col.name.lower().startswith(passthrough_prefix) -%}
            {%- do clinical_extension_columns.append({"name": col.name, "data_type": col.data_type}) -%}
        {%- endif -%}
    {%- endfor -%}
{%- else -%}
    {%- set clinical_extension_columns = [] -%}
{%- endif -%}

{%- set claims_patient_extension_columns -%}
    {%- for clinical_col in clinical_extension_columns %}
        , cast(null as {{ clinical_col["data_type"] }}) as {{ adapter.quote(clinical_col["name"]) }}
    {%- endfor -%}
{%- endset -%}

{%- set clinical_patient_extension_columns -%}
    {%- for clinical_col in clinical_extension_columns %}
        , clinical_patient.{{ adapter.quote(clinical_col["name"]) }}
    {%- endfor -%}
{%- endset -%}

{%- set unioned_extension_columns -%}
    {%- for clinical_col in clinical_extension_columns %}
        , unioned.{{ adapter.quote(clinical_col["name"]) }}
    {%- endfor -%}
{%- endset -%}

{%- set final_extension_columns -%}
    {{ select_extension_columns(ref('core__int_patient_remove_duplicates'), alias='patient_base') }}
{%- endset -%}

with claims_patient as (
    select
        *
    from {{ ref('normalized__eligibility_remove_duplicates') }}
)

, person_list_to_exclude_because_in_claims as (
    select distinct
          person_id
        , data_source
    from claims_patient
)

, clinical_patient as (
    select
        *
    from {{ ref('core__int_patient_remove_duplicates') }}
)

, unioned as (
    select
          1 as _source
        , claims_patient.person_id
        , claims_patient.name_suffix
        , claims_patient.first_name
        , claims_patient.middle_name
        , claims_patient.last_name
        , claims_patient.sex
        , claims_patient.race
        , claims_patient.birth_date
        , claims_patient.death_date
        , claims_patient.death_flag
        , claims_patient.social_security_number
        , claims_patient.address
        , claims_patient.city
        , claims_patient.state
        , claims_patient.zip_code
        , claims_patient.county
        , claims_patient.latitude
        , claims_patient.longitude
        , claims_patient.phone
        , claims_patient.email
        , claims_patient.ethnicity
        {{ claims_patient_extension_columns }}
        , claims_patient.data_source
        , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
        , claims_patient.tuva_last_run
    from claims_patient

    union all

    select
          2 as _source
        , clinical_patient.person_id
        , clinical_patient.name_suffix
        , clinical_patient.first_name
        , clinical_patient.middle_name
        , clinical_patient.last_name
        , clinical_patient.sex
        , clinical_patient.race
        , clinical_patient.birth_date
        , clinical_patient.death_date
        , clinical_patient.death_flag
        , clinical_patient.social_security_number
        , clinical_patient.address
        , clinical_patient.city
        , clinical_patient.state
        , clinical_patient.zip_code
        , clinical_patient.county
        , clinical_patient.latitude
        , clinical_patient.longitude
        , clinical_patient.phone
        , clinical_patient.email
        , clinical_patient.ethnicity
        {{ clinical_patient_extension_columns }}
        , clinical_patient.data_source
        , clinical_patient.ingest_datetime
        , clinical_patient.tuva_last_run
    from clinical_patient
)

, patient_base as (
    select
          unioned.person_id
        , unioned.name_suffix
        , unioned.first_name
        , unioned.middle_name
        , unioned.last_name
        , unioned.sex
        , unioned.race
        , unioned.birth_date
        , unioned.death_date
        , unioned.death_flag
        , unioned.social_security_number
        , unioned.address
        , unioned.city
        , unioned.state
        , unioned.zip_code
        , unioned.county
        , unioned.latitude
        , unioned.longitude
        , unioned.phone
        , unioned.email
        , unioned.ethnicity
        {{ unioned_extension_columns }}
        , unioned.data_source
        , unioned.ingest_datetime
        , unioned.tuva_last_run
        , cast(substring(cast(unioned.tuva_last_run as {{ dbt.type_string() }}), 1, 10) as date) as tuva_last_run_date
    from unioned
    where _source = 1

    union all

    select
          unioned.person_id
        , unioned.name_suffix
        , unioned.first_name
        , unioned.middle_name
        , unioned.last_name
        , unioned.sex
        , unioned.race
        , unioned.birth_date
        , unioned.death_date
        , unioned.death_flag
        , unioned.social_security_number
        , unioned.address
        , unioned.city
        , unioned.state
        , unioned.zip_code
        , unioned.county
        , unioned.latitude
        , unioned.longitude
        , unioned.phone
        , unioned.email
        , unioned.ethnicity
        {{ unioned_extension_columns }}
        , unioned.data_source
        , unioned.ingest_datetime
        , unioned.tuva_last_run
        , cast(substring(cast(unioned.tuva_last_run as {{ dbt.type_string() }}), 1, 10) as date) as tuva_last_run_date
    from unioned
    left outer join person_list_to_exclude_because_in_claims as claims_people
        on unioned.person_id = claims_people.person_id
        and unioned.data_source = claims_people.data_source
    where _source = 2
      and claims_people.person_id is null
)

select
    {{ final_core_columns }}
    , {{ age_expression }} as age
    , {{ age_group_expression }} as age_group
    {{ final_extension_columns }}
    {{ final_metadata_columns }}
from patient_base

{% elif the_tuva_project.tuva_boolean_var('clinical_enabled', false) == true -%}

{%- set source_extension_columns -%}
    {{ select_extension_columns(ref('core__int_patient_remove_duplicates'), alias='patient_source', strip_prefix=false) }}
{%- endset -%}

{%- set final_extension_columns -%}
    {{ select_extension_columns(ref('core__int_patient_remove_duplicates'), alias='patient_base') }}
{%- endset -%}

with patient_base as (
    select
          patient_source.person_id
        , patient_source.name_suffix
        , patient_source.first_name
        , patient_source.middle_name
        , patient_source.last_name
        , patient_source.sex
        , patient_source.race
        , patient_source.birth_date
        , patient_source.death_date
        , patient_source.death_flag
        , patient_source.social_security_number
        , patient_source.address
        , patient_source.city
        , patient_source.state
        , patient_source.zip_code
        , patient_source.county
        , patient_source.latitude
        , patient_source.longitude
        , patient_source.phone
        , patient_source.email
        , patient_source.ethnicity
        {{ source_extension_columns }}
        , patient_source.data_source
        , patient_source.ingest_datetime
        , patient_source.tuva_last_run
        , cast(substring(cast(patient_source.tuva_last_run as {{ dbt.type_string() }}), 1, 10) as date) as tuva_last_run_date
    from {{ ref('core__int_patient_remove_duplicates') }} as patient_source
)

select
    {{ final_core_columns }}
    , {{ age_expression }} as age
    , {{ age_group_expression }} as age_group
    {{ final_extension_columns }}
    {{ final_metadata_columns }}
from patient_base

{% elif the_tuva_project.tuva_boolean_var('claims_enabled', false) == true -%}

with patient_base as (
    select
          patient_source.person_id
        , patient_source.name_suffix
        , patient_source.first_name
        , patient_source.middle_name
        , patient_source.last_name
        , patient_source.sex
        , patient_source.race
        , patient_source.birth_date
        , patient_source.death_date
        , patient_source.death_flag
        , patient_source.social_security_number
        , patient_source.address
        , patient_source.city
        , patient_source.state
        , patient_source.zip_code
        , patient_source.county
        , patient_source.latitude
        , patient_source.longitude
        , patient_source.phone
        , patient_source.email
        , patient_source.ethnicity
        , patient_source.data_source
        , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
        , patient_source.tuva_last_run
        , cast(substring(cast(patient_source.tuva_last_run as {{ dbt.type_string() }}), 1, 10) as date) as tuva_last_run_date
    from {{ ref('normalized__eligibility_remove_duplicates') }} as patient_source
)

select
    {{ final_core_columns }}
    , {{ age_expression }} as age
    , {{ age_group_expression }} as age_group
    {{ final_metadata_columns }}
from patient_base

{%- endif %}
