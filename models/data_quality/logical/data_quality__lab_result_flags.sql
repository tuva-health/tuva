{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'lab_result_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set source_component_type_valid_where = "lower(cast(source_rows.source_component_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct', 'local', 'unknown')" %}

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

encounter_rows as (
    select distinct
          encounter_id
        , data_source
    from {{ ref('input_layer__encounter') }}
),

final as (
    select
          source_rows.lab_result_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.accession_number
        , source_rows.source_component_type
        , source_rows.source_component_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.accession_number is null") }} as accession_number_null
        , {{ dq_logical_int_flag_sql("source_rows.source_component_type is not null and not (" ~ source_component_type_valid_where ~ ")") }} as source_component_type_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_component_code is not null "
              ~ "and " ~ source_component_type_valid_where ~ " "
              ~ "and lower(cast(source_rows.source_component_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct') "
              ~ "and loinc_lookup.loinc is null "
              ~ "and snomed_ct_lookup.snomed_ct is null"
          ) }} as source_component_code_invalid
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
    left join {{ ref('terminology__loinc') }} as loinc_lookup
        on lower(cast(source_rows.source_component_type as {{ string_type }})) = 'loinc'
       and cast(source_rows.source_component_code as {{ string_type }}) = cast(loinc_lookup.loinc as {{ string_type }})
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_lookup
        on lower(cast(source_rows.source_component_type as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_component_code as {{ string_type }}) = cast(snomed_ct_lookup.snomed_ct as {{ string_type }})
)

select *
from final
