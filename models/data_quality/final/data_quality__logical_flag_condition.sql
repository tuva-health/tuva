{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool)
   )
}}

{% set string_type = dbt.type_string() %}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__condition') }}
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
          source_rows.source_condition_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.code_system
        , source_rows.source_code
        , source_rows.present_on_admit_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.source_code is null") }} as source_code_null
        , {{ dq_logical_int_flag_sql("source_rows.code_system is null") }} as code_system_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.code_system is not null and lower(cast(source_rows.code_system as " ~ string_type ~ ")) not in ('icd-9-cm', 'icd-10-cm', 'snomed-ct', 'unknown')") }} as code_system_invalid
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and lower(cast(source_rows.code_system as " ~ string_type ~ ")) in ('icd-10-cm', 'icd-9-cm', 'snomed-ct') "
              ~ "and icd_10_cm_lookup.icd_10_cm is null "
              ~ "and icd_9_cm_lookup.icd_9_cm is null "
              ~ "and snomed_ct_lookup.snomed_ct is null"
          ) }} as source_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.present_on_admit_code is not null and present_on_admit_lookup.present_on_admit_code is null") }} as present_on_admit_code_invalid
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
