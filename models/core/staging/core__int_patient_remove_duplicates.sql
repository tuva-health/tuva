{{ config(
     enabled = var('clinical_enabled', var('tuva_marts_enabled', False)) | as_bool
   )
}}

{%- set tuva_core_columns -%}
      patient_source.person_id
    , patient_source.patient_id
    , patient_source.first_name
    , patient_source.middle_name
    , patient_source.last_name
    , patient_source.name_suffix
    , patient_source.sex
    , patient_source.race
    , patient_source.ethnicity
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
{%- endset -%}

{%- set tuva_extension_columns -%}
    {{ select_extension_columns(ref('core__int_patient_casting'), alias='patient_source', strip_prefix=false) }}
{%- endset -%}

{%- set tuva_metadata_columns -%}
    , patient_source.ingest_datetime
    , patient_source.tuva_last_run
    , patient_source.data_source
{%- endset -%}

with patient_source as (
    select
          patient_casting.*
        , row_number() over (
            partition by patient_casting.person_id, patient_casting.data_source
            order by
                case when patient_casting.ingest_datetime is null then 1 else 0 end
                , patient_casting.ingest_datetime desc
                , patient_casting.patient_id desc
        ) as row_sequence
    from {{ ref('core__int_patient_casting') }} as patient_casting
)

select
    {{ tuva_core_columns }}
    {{ tuva_extension_columns }}
    {{ tuva_metadata_columns }}
from patient_source
where row_sequence = 1
