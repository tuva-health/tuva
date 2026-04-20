{{ config(
     enabled = (var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool)
            or (var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool)
   )
}}

{% if var('clinical_enabled', var('tuva_marts_enabled', False)) == true
   and var('claims_enabled', var('tuva_marts_enabled', False)) == true -%}

{%- set claims_source_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='elig', strip_prefix=false) }}
{%- endset -%}

{%- set claims_stage_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='claims_patient', strip_prefix=false) }}
{%- endset -%}

{%- set claims_latest_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='claims_patient_source', strip_prefix=false) }}
{%- endset -%}

{%- set tuva_core_columns -%}
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
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , unioned.data_source
    , unioned.file_name
    , unioned.ingest_datetime
    , unioned.tuva_last_run
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='unioned', strip_prefix=false) }}
{%- endset -%}

{%- if execute -%}
    {%- set passthrough_config = var('passthrough', {}) -%}
    {%- set passthrough_prefix = passthrough_config.get('prefix', 'x_').lower() -%}
    {%- set claims_extension_on_clinical_columns = [] -%}
    {%- set clinical_column_names = adapter.get_columns_in_relation(ref('normalized_input__patient')) | map(attribute='name') | map('lower') | list -%}
    {%- for col in adapter.get_columns_in_relation(ref('normalized_input__eligibility')) -%}
        {%- if col.name.lower().startswith(passthrough_prefix) -%}
            {%- if col.name.lower() in clinical_column_names -%}
                {%- do claims_extension_on_clinical_columns.append("clinical_patient." ~ col.name) -%}
            {%- else -%}
                {%- do claims_extension_on_clinical_columns.append("cast(null as " ~ col.data_type ~ ") as " ~ col.name) -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- else -%}
    {%- set claims_extension_on_clinical_columns = [] -%}
{%- endif -%}

with claims_patient_source as (
    select
          cast(elig.person_id as {{ dbt.type_string() }}) as person_id
        , cast(elig.name_suffix as {{ dbt.type_string() }}) as name_suffix
        , cast(elig.first_name as {{ dbt.type_string() }}) as first_name
        , cast(elig.middle_name as {{ dbt.type_string() }}) as middle_name
        , cast(elig.last_name as {{ dbt.type_string() }}) as last_name
        , cast(elig.gender as {{ dbt.type_string() }}) as sex
        , cast(elig.race as {{ dbt.type_string() }}) as race
        , elig.birth_date as birth_date
        , elig.death_date as death_date
        , cast(elig.death_flag as {{ dbt.type_int() }}) as death_flag
        , cast(elig.social_security_number as {{ dbt.type_string() }}) as social_security_number
        , cast(elig.address as {{ dbt.type_string() }}) as address
        , cast(elig.city as {{ dbt.type_string() }}) as city
        , cast(elig.state as {{ dbt.type_string() }}) as state
        , cast(elig.zip_code as {{ dbt.type_string() }}) as zip_code
        , cast(null as {{ dbt.type_string() }}) as county
        , cast(null as {{ dbt.type_numeric() }}) as latitude
        , cast(null as {{ dbt.type_numeric() }}) as longitude
        , cast(elig.phone as {{ dbt.type_string() }}) as phone
        , cast(elig.email as {{ dbt.type_string() }}) as email
        , cast(elig.ethnicity as {{ dbt.type_string() }}) as ethnicity
        {{ claims_source_extension_columns }}
        , cast(elig.data_source as {{ dbt.type_string() }}) as data_source
        , cast(elig.file_name as {{ dbt.type_string() }}) as file_name
        , cast(elig.ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
        , elig.tuva_last_run as tuva_last_run
        , row_number() over (
            partition by elig.person_id
            order by case
                when elig.enrollment_end_date is null then cast('2050-01-01' as date)
                else elig.enrollment_end_date
            end desc
        ) as row_sequence
    from {{ ref('normalized_input__eligibility') }} as elig
)

, claims_patient as (
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
        {{ claims_latest_extension_columns }}
        , data_source
        , file_name
        , ingest_datetime
        , tuva_last_run
    from claims_patient_source
    where row_sequence = 1
)

, person_list_to_exclude_because_in_claims as (
    select distinct person_id
    from claims_patient
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
        {{ claims_stage_extension_columns }}
        , claims_patient.data_source
        , claims_patient.file_name
        , claims_patient.ingest_datetime
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
        {%- for col_expr in claims_extension_on_clinical_columns %}
        , {{ col_expr }}
        {%- endfor %}
        , clinical_patient.data_source
        , clinical_patient.file_name
        , clinical_patient.ingest_datetime
        , clinical_patient.tuva_last_run
    from {{ ref('normalized_input__patient') }} as clinical_patient
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from unioned
where _source = 1

union all

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from unioned
left outer join person_list_to_exclude_because_in_claims as claims_people
    on unioned.person_id = claims_people.person_id
where _source = 2
  and claims_people.person_id is null

{% elif var('clinical_enabled', var('tuva_marts_enabled', False)) == true -%}

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

{%- set tuva_metadata_columns -%}
    , data_source
    , file_name
    , ingest_datetime
    , tuva_last_run
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__patient'), strip_prefix=false) }}
{%- endset -%}

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from {{ ref('normalized_input__patient') }}

{% elif var('claims_enabled', var('tuva_marts_enabled', False)) == true -%}

{%- set claims_source_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='elig', strip_prefix=false) }}
{%- endset -%}

{%- set claims_stage_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='claims_patient', strip_prefix=false) }}
{%- endset -%}

{%- set claims_latest_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='claims_patient_source', strip_prefix=false) }}
{%- endset -%}

{%- set tuva_core_columns -%}
      claims_patient.person_id
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
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , claims_patient.data_source
    , claims_patient.file_name
    , claims_patient.ingest_datetime
    , claims_patient.tuva_last_run
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('normalized_input__eligibility'), alias='claims_patient', strip_prefix=false) }}
{%- endset -%}

with claims_patient_source as (
    select
          cast(elig.person_id as {{ dbt.type_string() }}) as person_id
        , cast(elig.name_suffix as {{ dbt.type_string() }}) as name_suffix
        , cast(elig.first_name as {{ dbt.type_string() }}) as first_name
        , cast(elig.middle_name as {{ dbt.type_string() }}) as middle_name
        , cast(elig.last_name as {{ dbt.type_string() }}) as last_name
        , cast(elig.gender as {{ dbt.type_string() }}) as sex
        , cast(elig.race as {{ dbt.type_string() }}) as race
        , elig.birth_date as birth_date
        , elig.death_date as death_date
        , cast(elig.death_flag as {{ dbt.type_int() }}) as death_flag
        , cast(elig.social_security_number as {{ dbt.type_string() }}) as social_security_number
        , cast(elig.address as {{ dbt.type_string() }}) as address
        , cast(elig.city as {{ dbt.type_string() }}) as city
        , cast(elig.state as {{ dbt.type_string() }}) as state
        , cast(elig.zip_code as {{ dbt.type_string() }}) as zip_code
        , cast(null as {{ dbt.type_string() }}) as county
        , cast(null as {{ dbt.type_numeric() }}) as latitude
        , cast(null as {{ dbt.type_numeric() }}) as longitude
        , cast(elig.phone as {{ dbt.type_string() }}) as phone
        , cast(elig.email as {{ dbt.type_string() }}) as email
        , cast(elig.ethnicity as {{ dbt.type_string() }}) as ethnicity
        {{ claims_source_extension_columns }}
        , cast(elig.data_source as {{ dbt.type_string() }}) as data_source
        , cast(elig.file_name as {{ dbt.type_string() }}) as file_name
        , cast(elig.ingest_datetime as {{ dbt.type_timestamp() }}) as ingest_datetime
        , elig.tuva_last_run as tuva_last_run
        , row_number() over (
            partition by elig.person_id
            order by case
                when elig.enrollment_end_date is null then cast('2050-01-01' as date)
                else elig.enrollment_end_date
            end desc
        ) as row_sequence
    from {{ ref('normalized_input__eligibility') }} as elig
)

, claims_patient as (
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
        {{ claims_latest_extension_columns }}
        , data_source
        , file_name
        , ingest_datetime
        , tuva_last_run
    from claims_patient_source
    where row_sequence = 1
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from claims_patient

{%- endif %}
