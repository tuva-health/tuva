{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'appointment_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('input_layer__appointment') }}
),

patient_rows as (
    select distinct
          person_id
        , patient_id
        , data_source
    from {{ ref('input_layer__patient') }}
),

patient_person_rows as (
    select distinct
          person_id
        , data_source
    from patient_rows
),

patient_patient_rows as (
    select distinct
          patient_id
        , data_source
    from patient_rows
),

encounter_rows as (
    select distinct
          encounter_id
        , person_id
        , patient_id
        , data_source
    from {{ ref('input_layer__encounter') }}
),

encounter_id_rows as (
    select distinct
          encounter_id
        , data_source
    from encounter_rows
),

appointment_type_rows as (
    select distinct code
    from {{ ref('terminology__appointment_type') }}
),

appointment_status_rows as (
    select distinct code
    from {{ ref('terminology__appointment_status') }}
),

final as (
    select
          source_rows.appointment_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql(
              "source_rows.person_id is not null and patient_person.person_id is null",
              "source_rows.person_id is not null"
          ) }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql(
              "source_rows.patient_id is not null and patient_patient.patient_id is null",
              "source_rows.patient_id is not null"
          ) }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql(
              "patient_pair.person_id is null",
              "source_rows.person_id is not null "
              ~ "and source_rows.patient_id is not null "
              ~ "and patient_person.person_id is not null "
              ~ "and patient_patient.patient_id is not null"
          ) }} as person_patient_pair_not_in_patient
        , {{ dq_logical_int_flag_sql(
              "source_rows.encounter_id is not null and encounter_id_rows.encounter_id is null",
              "source_rows.encounter_id is not null"
          ) }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql(
              "encounter_pair.encounter_id is null",
              "source_rows.encounter_id is not null "
              ~ "and encounter_id_rows.encounter_id is not null "
              ~ "and patient_pair.person_id is not null"
          ) }} as encounter_person_patient_pair_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.start_datetime is null", "1 = 1") }} as start_datetime_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.end_datetime < source_rows.start_datetime",
              "source_rows.start_datetime is not null and source_rows.end_datetime is not null"
          ) }} as end_datetime_before_start_datetime
        , {{ dq_logical_int_flag_sql(
              "source_rows.duration < 0",
              "source_rows.duration is not null"
          ) }} as duration_negative
        , {{ dq_logical_int_flag_sql(
              "appointment_type_rows.code is null",
              "source_rows.type_code is not null"
          ) }} as type_code_invalid
        , {{ dq_logical_int_flag_sql(
              "appointment_status_rows.code is null",
              "source_rows.status_code is not null"
          ) }} as status_code_invalid
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
    from source_rows
    left join patient_person_rows as patient_person
        on source_rows.person_id = patient_person.person_id
       and source_rows.data_source = patient_person.data_source
    left join patient_patient_rows as patient_patient
        on source_rows.patient_id = patient_patient.patient_id
       and source_rows.data_source = patient_patient.data_source
    left join patient_rows as patient_pair
        on source_rows.person_id = patient_pair.person_id
       and source_rows.patient_id = patient_pair.patient_id
       and source_rows.data_source = patient_pair.data_source
    left join encounter_id_rows
        on source_rows.encounter_id = encounter_id_rows.encounter_id
       and source_rows.data_source = encounter_id_rows.data_source
    left join encounter_rows as encounter_pair
        on source_rows.encounter_id = encounter_pair.encounter_id
       and source_rows.person_id = encounter_pair.person_id
       and source_rows.patient_id = encounter_pair.patient_id
       and source_rows.data_source = encounter_pair.data_source
    left join appointment_type_rows
        on lower(trim(cast(source_rows.type_code as {{ string_type }}))) = lower(trim(cast(appointment_type_rows.code as {{ string_type }})))
    left join appointment_status_rows
        on lower(trim(cast(source_rows.status_code as {{ string_type }}))) = lower(trim(cast(appointment_status_rows.code as {{ string_type }})))
)

select *
from final
