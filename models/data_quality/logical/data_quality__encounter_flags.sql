{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'encounter_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_encounter_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set diagnosis_code_type_valid_where = "lower(cast(source_rows.primary_diagnosis_code_type as " ~ string_type ~ ")) in ('icd-10-cm', 'icd-9-cm')" %}
{% set diagnosis_code_type_invalid_where = "source_rows.primary_diagnosis_code_type is not null and lower(cast(source_rows.primary_diagnosis_code_type as " ~ string_type ~ ")) not in ('icd-10-cm', 'icd-9-cm')" %}
{% set drg_code_type_valid_where = "lower(cast(source_rows.drg_code_type as " ~ string_type ~ ")) in ('ms-drg', 'apr-drg')" %}
{% set drg_code_type_invalid_where = "source_rows.drg_code_type is not null and lower(cast(source_rows.drg_code_type as " ~ string_type ~ ")) not in ('ms-drg', 'apr-drg')" %}

with source_rows as (
    select *
    from {{ ref('input_layer__encounter') }}
),

patient_rows as (
    select distinct
          person_id
        , patient_id
        , data_source
    from {{ ref('input_layer__patient') }}
),

final as (
    select
          source_rows.encounter_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_type
        , source_rows.encounter_start_date
        , source_rows.encounter_end_date
        , source_rows.admit_source_code
        , source_rows.admit_type_code
        , source_rows.discharge_disposition_code
        , source_rows.facility_npi
        , source_rows.primary_diagnosis_code_type
        , source_rows.primary_diagnosis_code
        , source_rows.drg_code_type
        , source_rows.drg_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_type is not null and encounter_type_lookup.encounter_type is null") }} as encounter_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.encounter_start_date is null") }} as encounter_start_date_null
        , {{ dq_logical_int_flag_sql("source_rows.encounter_end_date is null") }} as encounter_end_date_null
        , {{ dq_logical_int_flag_sql("source_rows.encounter_start_date is not null and (source_rows.encounter_start_date < " ~ min_encounter_date_sql ~ " or source_rows.encounter_start_date > " ~ current_date_sql ~ ")") }} as encounter_start_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.encounter_end_date is not null and (source_rows.encounter_end_date < " ~ min_encounter_date_sql ~ " or source_rows.encounter_end_date > " ~ current_date_sql ~ ")") }} as encounter_end_date_out_of_reasonable_range
        , {{ dq_logical_int_flag_sql("source_rows.admit_source_code is not null and admit_source_lookup.admit_source_code is null") }} as admit_source_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.admit_type_code is not null and admit_type_lookup.admit_type_code is null") }} as admit_type_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.discharge_disposition_code is not null and discharge_disposition_lookup.discharge_disposition_code is null") }} as discharge_disposition_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.facility_npi is not null and facility_provider_lookup.npi is null") }} as facility_npi_invalid
        , {{ dq_logical_int_flag_sql("source_rows.primary_diagnosis_code_type is null") }} as primary_diagnosis_code_type_null
        , {{ dq_logical_int_flag_sql(diagnosis_code_type_invalid_where) }} as primary_diagnosis_code_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.primary_diagnosis_code is null") }} as primary_diagnosis_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.primary_diagnosis_code is not null "
              ~ "and " ~ diagnosis_code_type_valid_where ~ " "
              ~ "and icd_10_cm_lookup.icd_10_cm is null "
              ~ "and icd_9_cm_lookup.icd_9_cm is null"
          ) }} as primary_diagnosis_code_invalid
        , {{ dq_logical_int_flag_sql("source_rows.drg_code_type is null") }} as drg_code_type_null
        , {{ dq_logical_int_flag_sql(drg_code_type_invalid_where) }} as drg_code_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.drg_code is null") }} as drg_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.drg_code is not null "
              ~ "and " ~ drg_code_type_valid_where ~ " "
              ~ "and ms_drg_lookup.ms_drg_code is null "
              ~ "and apr_drg_lookup.apr_drg_code is null"
          ) }} as drg_code_invalid
    from source_rows
    left join patient_rows as patient_person
        on source_rows.person_id = patient_person.person_id
       and source_rows.data_source = patient_person.data_source
    left join patient_rows as patient_patient
        on source_rows.patient_id = patient_patient.patient_id
       and source_rows.data_source = patient_patient.data_source
    left join {{ ref('terminology__encounter_type') }} as encounter_type_lookup
        on lower(cast(source_rows.encounter_type as {{ string_type }})) = lower(cast(encounter_type_lookup.encounter_type as {{ string_type }}))
    left join {{ ref('terminology__admit_source') }} as admit_source_lookup
        on cast(source_rows.admit_source_code as {{ string_type }}) = cast(admit_source_lookup.admit_source_code as {{ string_type }})
    left join {{ ref('terminology__admit_type') }} as admit_type_lookup
        on cast(source_rows.admit_type_code as {{ string_type }}) = cast(admit_type_lookup.admit_type_code as {{ string_type }})
    left join {{ ref('terminology__discharge_disposition') }} as discharge_disposition_lookup
        on cast(source_rows.discharge_disposition_code as {{ string_type }}) = cast(discharge_disposition_lookup.discharge_disposition_code as {{ string_type }})
    left join {{ ref('provider_data__provider') }} as facility_provider_lookup
        on cast(source_rows.facility_npi as {{ string_type }}) = cast(facility_provider_lookup.npi as {{ string_type }})
    left join {{ ref('terminology__icd_10_cm') }} as icd_10_cm_lookup
        on lower(cast(source_rows.primary_diagnosis_code_type as {{ string_type }})) = 'icd-10-cm'
       and replace(cast(source_rows.primary_diagnosis_code as {{ string_type }}), '.', '') = replace(cast(icd_10_cm_lookup.icd_10_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__icd_9_cm') }} as icd_9_cm_lookup
        on lower(cast(source_rows.primary_diagnosis_code_type as {{ string_type }})) = 'icd-9-cm'
       and replace(cast(source_rows.primary_diagnosis_code as {{ string_type }}), '.', '') = replace(cast(icd_9_cm_lookup.icd_9_cm as {{ string_type }}), '.', '')
    left join {{ ref('terminology__ms_drg') }} as ms_drg_lookup
        on lower(cast(source_rows.drg_code_type as {{ string_type }})) = 'ms-drg'
       and cast(source_rows.drg_code as {{ string_type }}) = cast(ms_drg_lookup.ms_drg_code as {{ string_type }})
    left join {{ ref('terminology__apr_drg') }} as apr_drg_lookup
        on lower(cast(source_rows.drg_code_type as {{ string_type }})) = 'apr-drg'
       and cast(source_rows.drg_code as {{ string_type }}) = cast(apr_drg_lookup.apr_drg_code as {{ string_type }})
)

select *
from final
