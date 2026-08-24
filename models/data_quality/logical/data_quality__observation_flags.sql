{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'observation_flags',
     tags = ['data_quality', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_event_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set source_code_type_standard_where = "lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct', 'icd-10-cm', 'icd-9-cm', 'icd-10-pcs', 'icd-9-pcs')" %}

with source_rows as (
    select *
    from {{ ref('input_layer__observation') }}
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

observation_type_rows as (
    select distinct
          observation_type
    from {{ ref('terminology__observation_type') }}
),

final as (
    select
          source_rows.observation_id
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
        , {{ dq_logical_int_flag_sql("source_rows.observation_date is null", "1 = 1") }} as observation_date_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.observation_date is not null and (source_rows.observation_date < " ~ min_event_date_sql ~ " or source_rows.observation_date > " ~ current_date_sql ~ ")",
              "source_rows.observation_date is not null"
          ) }} as observation_date_out_of_range
        , {{ dq_logical_ingest_datetime_range_flag_sql(
              "source_rows.ingest_datetime"
          ) }} as ingest_datetime_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.observation_type is not null and observation_type_rows.observation_type is null", "source_rows.observation_type is not null") }} as observation_type_invalid
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
              ~ "and " ~ source_code_type_standard_where ~ " "
              ~ "and loinc_lookup.loinc is null "
              ~ "and snomed_ct_lookup.snomed_ct is null "
              ~ "and icd_10_cm_lookup.icd_10_cm is null "
              ~ "and icd_9_cm_lookup.icd_9_cm is null "
              ~ "and icd_10_pcs_lookup.icd_10_pcs is null "
              ~ "and icd_9_pcs_lookup.icd_9_pcs is null",
              "source_rows.source_code is not null and " ~ source_code_type_standard_where
          ) }} as source_code_invalid
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
    left join observation_type_rows
        on lower(cast(source_rows.observation_type as {{ string_type }})) = lower(cast(observation_type_rows.observation_type as {{ string_type }}))
    left join {{ ref('terminology__loinc') }} as loinc_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'loinc'
       and cast(source_rows.source_code as {{ string_type }}) = cast(loinc_lookup.loinc as {{ string_type }})
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_code as {{ string_type }}) = cast(snomed_ct_lookup.snomed_ct as {{ string_type }})
    left join {{ ref('terminology__icd_10_cm') }} as icd_10_cm_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'icd-10-cm'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_10_cm_lookup.icd_10_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_9_cm') }} as icd_9_cm_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'icd-9-cm'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_9_cm_lookup.icd_9_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_10_pcs') }} as icd_10_pcs_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'icd-10-pcs'
       and cast(source_rows.source_code as {{ string_type }}) = cast(icd_10_pcs_lookup.icd_10_pcs as {{ string_type }})
    left join {{ ref('terminology__icd_9_pcs') }} as icd_9_pcs_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'icd-9-pcs'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_9_pcs_lookup.icd_9_pcs as {{ string_type }}), '.', '')
)

select *
from final
