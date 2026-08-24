{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'lab_result_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set date_type = api.Column.translate_type('date') %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_lab_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set result_date_sql = "cast(source_rows.result_datetime as " ~ date_type ~ ")" %}
{% set collection_date_sql = "cast(source_rows.collection_datetime as " ~ date_type ~ ")" %}
{% set source_order_type_standard_where = "lower(cast(source_rows.source_order_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct')" %}

with source_rows as (
    select *
    from {{ ref('input_layer__lab_result') }}
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
          source_rows.lab_result_id
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
        , {{ dq_logical_int_flag_sql("source_rows.accession_number is null", "1 = 1") }} as accession_number_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_component_code is not null and source_rows.source_component_type is null",
              "source_rows.source_component_code is not null"
          ) }} as source_component_type_null_when_source_component_code_present
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_component_code is not null "
              ~ "and lower(cast(source_rows.source_component_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct') "
              ~ "and loinc_lookup.loinc is null "
              ~ "and snomed_ct_lookup.snomed_ct is null",
              "source_rows.source_component_code is not null "
              ~ "and lower(cast(source_rows.source_component_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct')"
          ) }} as source_component_code_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_order_type is null",
              "source_rows.source_order_code is not null"
          ) }} as source_order_type_null_when_source_order_code_present
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_order_code is null",
              "source_rows.source_order_type is not null"
          ) }} as source_order_code_null_when_source_order_type_present
        , {{ dq_logical_int_flag_sql(
              "order_loinc_lookup.loinc is null and order_snomed_ct_lookup.snomed_ct is null",
              "source_rows.source_order_code is not null and " ~ source_order_type_standard_where
          ) }} as source_order_code_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.collection_datetime > source_rows.result_datetime",
              "source_rows.collection_datetime is not null and source_rows.result_datetime is not null"
          ) }} as collection_datetime_after_result_datetime
        , {{ dq_logical_int_flag_sql(
              result_date_sql ~ " < " ~ min_lab_date_sql ~ " or " ~ result_date_sql ~ " > " ~ current_date_sql,
              "source_rows.result_datetime is not null"
          ) }} as result_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql(
              collection_date_sql ~ " < " ~ min_lab_date_sql ~ " or " ~ collection_date_sql ~ " > " ~ current_date_sql,
              "source_rows.collection_datetime is not null"
          ) }} as collection_datetime_out_of_reasonable_range
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
    left join {{ ref('terminology__loinc') }} as loinc_lookup
        on lower(cast(source_rows.source_component_type as {{ string_type }})) = 'loinc'
       and cast(source_rows.source_component_code as {{ string_type }}) = cast(loinc_lookup.loinc as {{ string_type }})
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_lookup
        on lower(cast(source_rows.source_component_type as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_component_code as {{ string_type }}) = cast(snomed_ct_lookup.snomed_ct as {{ string_type }})
    left join {{ ref('terminology__loinc') }} as order_loinc_lookup
        on lower(cast(source_rows.source_order_type as {{ string_type }})) = 'loinc'
       and cast(source_rows.source_order_code as {{ string_type }}) = cast(order_loinc_lookup.loinc as {{ string_type }})
    left join {{ ref('terminology__snomed_ct') }} as order_snomed_ct_lookup
        on lower(cast(source_rows.source_order_type as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_order_code as {{ string_type }}) = cast(order_snomed_ct_lookup.snomed_ct as {{ string_type }})
)

select *
from final
