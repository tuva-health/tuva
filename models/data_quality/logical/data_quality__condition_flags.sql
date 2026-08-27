{{ config(
     enabled = (the_tuva_project.tuva_boolean_var('data_quality_enabled', false)) and (the_tuva_project.tuva_boolean_var('clinical_enabled', false)),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'condition_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_condition_date_sql = dq_date_literal_sql('1900-01-01') %}

with source_rows as (
    select *
    from {{ ref('input_layer__condition') }}
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

final as (
    select
          source_rows.source_condition_id
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null", "1 = 1") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null", "1 = 1") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.source_code is null", "1 = 1") }} as source_code_null
        , {{ dq_logical_int_flag_sql("source_rows.code_system is null", "1 = 1") }} as code_system_null
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
        , {{ dq_logical_int_flag_sql(
              "source_rows.onset_date > source_rows.resolved_date",
              "source_rows.onset_date is not null and source_rows.resolved_date is not null"
          ) }} as onset_date_after_resolved_date
        , {{ dq_logical_int_flag_sql(
              "source_rows.recorded_date < " ~ min_condition_date_sql ~ " or source_rows.recorded_date > " ~ current_date_sql,
              "source_rows.recorded_date is not null"
          ) }} as recorded_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.onset_date < " ~ min_condition_date_sql ~ " or source_rows.onset_date > " ~ current_date_sql,
              "source_rows.onset_date is not null"
          ) }} as onset_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.resolved_date < " ~ min_condition_date_sql ~ " or source_rows.resolved_date > " ~ current_date_sql,
              "source_rows.resolved_date is not null"
          ) }} as resolved_date_out_of_reasonable_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.code_system is not null and lower(cast(source_rows.code_system as " ~ string_type ~ ")) not in ('icd-9-cm', 'icd-10-cm', 'snomed-ct', 'unknown')",
              "source_rows.code_system is not null"
          ) }} as code_system_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.code_system as " ~ string_type ~ ")) in ('icd-10-cm', 'icd-9-cm', 'snomed-ct') "
              ~ "and icd_10_cm_lookup.icd_10_cm is null "
              ~ "and icd_9_cm_lookup.icd_9_cm is null "
              ~ "and snomed_ct_lookup.snomed_ct is null",
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.code_system as " ~ string_type ~ ")) in ('icd-10-cm', 'icd-9-cm', 'snomed-ct')"
          ) }} as source_code_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.present_on_admit_code is not null and present_on_admit_lookup.present_on_admit_code is null",
              "source_rows.present_on_admit_code is not null"
          ) }} as present_on_admit_code_invalid
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
    left join {{ ref('terminology__icd_10_cm') }} as icd_10_cm_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'icd-10-cm'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_10_cm_lookup.icd_10_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_9_cm') }} as icd_9_cm_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'icd-9-cm'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_9_cm_lookup.icd_9_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_code as {{ string_type }}) = cast(snomed_ct_lookup.snomed_ct as {{ string_type }})
    left join {{ ref('terminology__present_on_admission') }} as present_on_admit_lookup
        on cast(source_rows.present_on_admit_code as {{ string_type }}) = cast(present_on_admit_lookup.present_on_admit_code as {{ string_type }})
)

select *
from final
