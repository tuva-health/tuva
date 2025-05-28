{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select distinct
      cast(condition.person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(condition.condition_id as {{ dbt.type_string() }} ) as resource_internal_id
    , cast(condition.encounter_id as {{ dbt.type_string() }} ) as encounter_internal_id
    , 'encounter-diagnosis' as condition_category
    , cast(condition.recorded_date as {{ dbt.type_timestamp() }} ) as condition_recorded_datetime
    , coalesce(
          cast(condition.onset_date as {{ dbt.type_timestamp() }} )
        , cast(condition.recorded_date as {{ dbt.type_timestamp() }} )
      ) as condition_onset_datetime
    , cast(condition.resolved_date as {{ dbt.type_timestamp() }} ) as condition_abatement_datetime
    , cast(condition.status as {{ dbt.type_string() }} ) as condition_clinical_status
    , case
        when lower(condition.normalized_code_type) = 'icd-10-cm'
            and {{ length('condition.normalized_code') }} > 3
            then cast({{ concat_custom([
                    "substring(condition.normalized_code,1,3)",
                    "'.'",
                    "substring(condition.normalized_code,4)"
                    ]) }} as {{ dbt.type_string() }} )
        else cast(condition.normalized_code as {{ dbt.type_string() }} )
      end as condition_code
    , case
        when lower(condition.normalized_code_type) = 'icd-10-cm' then 'ICD10'
        when lower(condition.normalized_code_type) = 'icd-9-cm' then 'ICD9'
        else cast(condition.normalized_code_type as {{ dbt.type_string() }} )
      end as condition_code_system
    , 'finished' as encounter_status
    , case
        when encounter.encounter_group = 'inpatient' then 'IMP'
        when encounter.encounter_group in ('outpatient', 'office based') then 'AMB'
        else 'other'
      end as encounter_class_code
    , cast(encounter.encounter_start_date as {{ dbt.type_timestamp() }} ) as encounter_start_datetime
    , cast(encounter.encounter_end_date as {{ dbt.type_timestamp() }} ) as encounter_end_datetime
    , cast(condition.data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
    , cast('{{ the_tuva_project.get_tuva_package_version() }}' as {{ dbt.type_string() }} ) as tuva_package_version
from {{ ref('fhir_preprocessing__stg_core__condition') }} as condition
    left outer join {{ ref('fhir_preprocessing__stg_core__encounter') }} as encounter
        on condition.encounter_id = encounter.encounter_id
where condition.normalized_code_type is not null
and condition.claim_id is null /* claim conditions are included in the EOB resource */
