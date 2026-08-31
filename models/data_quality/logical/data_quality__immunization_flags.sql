{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'immunization_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_immunization_date_sql = dq_date_literal_sql('1900-01-01') %}

with source_rows as (
    select *
    from {{ ref('input_layer__immunization') }}
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

cvx_rows as (
    select distinct cvx_terminology.cvx
    from {{ ref('terminology__cvx') }} as cvx_terminology
),

immunization_status_rows as (
    select distinct status_code
    from {{ ref('terminology__immunization_status') }}
),

immunization_status_reason_rows as (
    select distinct reason_code
    from {{ ref('terminology__immunization_status_reason') }}
),

final as (
    select
          source_rows.immunization_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null", "1 = 1") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null", "1 = 1") }} as patient_id_null
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
        , {{ dq_logical_int_flag_sql("source_rows.occurrence_date is null", "1 = 1") }} as occurrence_date_null
        , {{ dq_logical_date_range_flag_sql(
              "source_rows.occurrence_date",
              min_immunization_date_sql,
              current_date_sql
          ) }} as occurrence_date_outside_supported_date_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code_type is null",
              "source_rows.source_code is not null"
          ) }} as source_code_type_null_when_source_code_present
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is null",
              "source_rows.source_code_type is not null"
          ) }} as source_code_null_when_source_code_type_present
        , {{ dq_logical_int_flag_sql(
              "cvx_rows.cvx is null",
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'cvx'"
          ) }} as source_code_invalid
        , {{ dq_logical_int_flag_sql(
              "immunization_status_rows.status_code is null",
              "source_rows.status is not null"
          ) }} as status_invalid
        , {{ dq_logical_int_flag_sql(
              "immunization_status_reason_rows.reason_code is null",
              "source_rows.status_reason is not null"
          ) }} as status_reason_invalid
        , {{ dq_logical_int_flag_sql(
              "lower(cast(source_rows.status as " ~ string_type ~ ")) <> 'not-done'",
              "source_rows.status_reason is not null "
              ~ "and immunization_status_rows.status_code is not null"
          ) }} as status_reason_present_for_completed_or_error_status
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
    left join cvx_rows
        on cast(source_rows.source_code as {{ string_type }}) = cast(cvx_rows.cvx as {{ string_type }})
       and lower(cast(source_rows.source_code_type as {{ string_type }})) = 'cvx'
    left join immunization_status_rows
        on cast(source_rows.status as {{ string_type }}) = cast(immunization_status_rows.status_code as {{ string_type }})
    left join immunization_status_reason_rows
        on cast(source_rows.status_reason as {{ string_type }}) = cast(immunization_status_reason_rows.reason_code as {{ string_type }})
)

select *
from final
