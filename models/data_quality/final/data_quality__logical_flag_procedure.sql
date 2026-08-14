{{ config(
     enabled = (var('enable_data_quality', false) | as_bool) and (var('clinical_enabled', false) | as_bool)
   )
}}

{% set string_type = dbt.type_string() %}
{% set current_date_sql = dq_current_date_sql() %}
{% set min_event_date_sql = dq_date_literal_sql('2000-01-01') %}
{% set code_system_valid_where = "lower(cast(source_rows.code_system as " ~ string_type ~ ")) in ('icd-10-pcs', 'icd-9-pcs', 'hcpcs', 'snomed-ct', 'unknown')" %}
{% set code_system_standard_where = "lower(cast(source_rows.code_system as " ~ string_type ~ ")) in ('icd-10-pcs', 'icd-9-pcs', 'hcpcs', 'snomed-ct')" %}

with source_rows as (
    select *
    from {{ ref('stg_input_layer__procedure') }}
),

patient_rows as (
    select *
    from {{ ref('stg_data_quality__patient_key') }}
),

encounter_rows as (
    select *
    from {{ ref('stg_data_quality__encounter_key') }}
),

practitioner_rows as (
    select distinct
          practitioner_id
        , data_source
    from {{ ref('stg_input_layer__practitioner') }}
),

final as (
    select
          source_rows.source_procedure_id
        , source_rows.person_id
        , source_rows.patient_id
        , source_rows.encounter_id
        , source_rows.practitioner_id
        , source_rows.procedure_date
        , source_rows.code_system
        , source_rows.source_code
        , source_rows.data_source
        , {{ dq_logical_int_flag_sql("source_rows.person_id is null") }} as person_id_null
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is null") }} as patient_id_null
        , {{ dq_logical_int_flag_sql("source_rows.person_id is not null and patient_person.person_id is null") }} as person_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.patient_id is not null and patient_patient.patient_id is null") }} as patient_id_not_in_patient
        , {{ dq_logical_int_flag_sql("source_rows.encounter_id is not null and encounter_rows.encounter_id is null") }} as encounter_id_not_in_encounter
        , {{ dq_logical_int_flag_sql("source_rows.practitioner_id is not null and practitioner_rows.practitioner_id is null") }} as practitioner_id_not_in_practitioner
        , {{ dq_logical_int_flag_sql("source_rows.procedure_date is null") }} as procedure_date_null
        , {{ dq_logical_int_flag_sql("source_rows.procedure_date is not null and (source_rows.procedure_date < " ~ min_event_date_sql ~ " or source_rows.procedure_date > " ~ current_date_sql ~ ")") }} as procedure_date_out_of_range
        , {{ dq_logical_int_flag_sql("source_rows.code_system is null") }} as code_system_null
        , {{ dq_logical_int_flag_sql("source_rows.code_system is not null and not (" ~ code_system_valid_where ~ ")") }} as code_system_invalid
        , {{ dq_logical_int_flag_sql("source_rows.source_code is null") }} as source_code_null
        , {{ dq_logical_int_flag_sql(
              "source_rows.source_code is not null "
              ~ "and " ~ code_system_standard_where ~ " "
              ~ "and icd_10_pcs_lookup.icd_10_pcs is null "
              ~ "and icd_9_pcs_lookup.icd_9_pcs is null "
              ~ "and hcpcs_lookup.hcpcs is null "
              ~ "and snomed_ct_lookup.snomed_ct is null"
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
    left join practitioner_rows
        on source_rows.practitioner_id = practitioner_rows.practitioner_id
       and source_rows.data_source = practitioner_rows.data_source
    left join {{ ref('terminology__icd_10_pcs') }} as icd_10_pcs_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'icd-10-pcs'
       and cast(source_rows.source_code as {{ string_type }}) = cast(icd_10_pcs_lookup.icd_10_pcs as {{ string_type }})
    left join {{ ref('terminology__icd_9_pcs') }} as icd_9_pcs_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'icd-9-pcs'
       and replace(cast(source_rows.source_code as {{ string_type }}), '.', '') = replace(cast(icd_9_pcs_lookup.icd_9_pcs as {{ string_type }}), '.', '')
    left join {{ ref('terminology__hcpcs_level_2') }} as hcpcs_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'hcpcs'
       and cast(source_rows.source_code as {{ string_type }}) = cast(hcpcs_lookup.hcpcs as {{ string_type }})
    left join {{ ref('terminology__snomed_ct') }} as snomed_ct_lookup
        on lower(cast(source_rows.code_system as {{ string_type }})) = 'snomed-ct'
       and cast(source_rows.source_code as {{ string_type }}) = cast(snomed_ct_lookup.snomed_ct as {{ string_type }})
)

select *
from final
