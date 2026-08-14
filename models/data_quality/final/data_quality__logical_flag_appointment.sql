{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool)
   )
}}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__appointment') }}
),

patient_rows as (
    select *
    from {{ ref('stg_data_quality__patient_key') }}
),

encounter_rows as (
    select *
    from {{ ref('stg_data_quality__encounter_key') }}
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
