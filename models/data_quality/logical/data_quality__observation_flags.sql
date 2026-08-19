{{ config(
     enabled = (var('data_quality_enabled', false) | as_bool) and (var('clinical_enabled', false) | as_bool),
     schema = (
       var('tuva_schema_prefix', None) ~ '_data_quality'
       if var('tuva_schema_prefix', None) is not none
       else 'data_quality'
     ),
     alias = 'observation_flags',
     tags = ['data_quality', 'dq', 'dq1', 'dq_logical'],
     materialized = 'table'
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_event_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set source_code_type_valid_where = "lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct', 'icd-10-cm', 'icd-9-cm', 'icd-10-pcs', 'icd-9-pcs', 'hcpcs', 'local', 'unknown')" %}
{% set source_code_type_standard_where = "lower(cast(source_rows.source_code_type as " ~ string_type ~ ")) in ('loinc', 'snomed-ct', 'icd-10-cm', 'icd-9-cm', 'icd-10-pcs', 'icd-9-pcs', 'hcpcs')" %}

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

encounter_rows as (
    select distinct
          encounter_id
        , data_source
    from {{ ref('input_layer__encounter') }}
),

observation_type_rows as (
    select distinct
          observation_type
    from {{ ref('terminology__observation_type') }}
),

final as (
    select
          source_rows.observation_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.observation_date
        , source_rows.observation_type
        , source_rows.source_code_type
        , source_rows.source_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.observation_date is null") }} as observation_date_null
        , {{ dq_logical_int_flag_sql("source_rows.observation_date is not null and (source_rows.observation_date < " ~ min_event_date_sql ~ " or source_rows.observation_date > " ~ current_date_sql ~ ")") }} as observation_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.observation_type is not null and observation_type_rows.observation_type is null") }} as observation_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.source_code is not null and source_rows.source_code_type is null") }} as source_code_type_null_when_source_code_present
        , {{ dq_logical_int_flag_sql("source_rows.source_code_type is not null and not (" ~ source_code_type_valid_where ~ ")") }} as source_code_type_invalid
        , {{ dq_logical_int_flag_sql("source_rows.source_code_type is not null and source_rows.source_code is null") }} as source_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and " ~ source_code_type_standard_where ~ " "
              ~ "and loinc_lookup.loinc is null "
              ~ "and snomed_ct_lookup.snomed_ct is null "
              ~ "and icd_10_cm_lookup.icd_10_cm is null "
              ~ "and icd_9_cm_lookup.icd_9_cm is null "
              ~ "and icd_10_pcs_lookup.icd_10_pcs is null "
              ~ "and icd_9_pcs_lookup.icd_9_pcs is null "
              ~ "and hcpcs_lookup.hcpcs is null"
          ) }} as source_code_invalid
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
    left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs_lookup
        on lower(cast(source_rows.source_code_type as {{ string_type }})) = 'hcpcs'
       and cast(source_rows.source_code as {{ string_type }}) = cast(hcpcs_lookup.hcpcs as {{ string_type }})
)

select *
from final
