{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'appointment_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

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

encounter_rows as (
    select distinct
          encounter_id
        , data_source
    from {{ ref('input_layer__encounter') }}
),

final as (
    select
          source_rows.appointment_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.start_datetime
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.start_datetime is null") }} as start_datetime_null
    from source_rows
    left join patient_rows as patient_person
        on source_rows.person_id = patient_person.person_id
       and source_rows.data_source = patient_person.data_source
    left join patient_rows as patient_patient
        on source_rows.patient_id = patient_patient.patient_id
       and source_rows.data_source = patient_patient.data_source
    left join encounter_rows
        on source_rows.encounter_id = encounter_rows.encounter_id
       and source_rows.data_source = encounter_rows.data_source
)

select *
from final
