{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'medication_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_event_date_sql = dq_date_literal_sql('2000-01-01') %}
with source_rows as (
    select *
    from {{ ref('input_layer__medication') }}
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

practitioner_rows as (
    select distinct
          practitioner_id
        , data_source
    from {{ ref('input_layer__practitioner') }}
),

ndc_rows as (
    select distinct
          ndc11 as ndc_lookup_code
    from {{ ref('terminology__coderx_packages') }}

    union

    select distinct
          replace(nullif(ndc, 'NULL'), '-', '') as ndc_lookup_code
    from {{ ref('terminology__coderx_packages') }}
    where nullif(ndc, 'NULL') is not null
),

final as (
    select
          source_rows.medication_id
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
        , {{ dq_logical_int_flag_sql("source_rows.practitioner_id is not null and practitioner_rows.practitioner_id is null", "source_rows.practitioner_id is not null") }} as practitioner_id_not_in_practitioner
        , {{ dq_logical_int_flag_sql(
              "source_rows.dispensing_date is not null and (source_rows.dispensing_date < " ~ min_event_date_sql ~ " or source_rows.dispensing_date > " ~ current_date_sql ~ ")",
              "source_rows.dispensing_date is not null"
          ) }} as dispensing_date_out_of_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.prescribing_date is not null and (source_rows.prescribing_date < " ~ min_event_date_sql ~ " or source_rows.prescribing_date > " ~ current_date_sql ~ ")",
              "source_rows.prescribing_date is not null"
          ) }} as prescribing_date_out_of_range
        , {{ dq_logical_int_flag_sql(
              "source_rows.prescribing_date is not null and source_rows.dispensing_date is not null and source_rows.prescribing_date > source_rows.dispensing_date",
              "source_rows.prescribing_date is not null and source_rows.dispensing_date is not null"
          ) }} as prescribing_date_after_dispensing_date
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null and source_rows.source_code_type is null",
              "source_rows.source_code is not null"
          ) }} as source_code_type_null_when_source_code_present
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code_type is not null and source_rows.source_code is null",
              "source_rows.source_code_type is not null"
          ) }} as source_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'ndc' "
              ~ "and source_ndc_rows.ndc_lookup_code is null",
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) = 'ndc'"
          ) }} as source_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.ndc_code is not null and ndc_rows.ndc_lookup_code is null", "source_rows.ndc_code is not null") }} as ndc_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.quantity is not null and source_rows.quantity < 0", "source_rows.quantity is not null") }} as quantity_negative
        , {{ dq_logical_int_flag_sql("source_rows.days_supply is not null and source_rows.days_supply < 0", "source_rows.days_supply is not null") }} as days_supply_negative
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
    left join practitioner_rows
        on source_rows.practitioner_id = practitioner_rows.practitioner_id
       and source_rows.data_source = practitioner_rows.data_source
    left join ndc_rows as source_ndc_rows
        on replace(cast(source_rows.source_code as {{ string_type }}), '-', '')
           = cast(source_ndc_rows.ndc_lookup_code as {{ string_type }})
       and lower(cast(source_rows.source_code_type as {{ string_type }})) = 'ndc'
    left join ndc_rows
        on replace(cast(source_rows.ndc_code as {{ string_type }}), '-', '')
           = cast(ndc_rows.ndc_lookup_code as {{ string_type }})
)

select *
from final
