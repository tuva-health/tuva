{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select distinct
      condition.person_id as patient_internal_id
    , condition.condition_id as resource_internal_id
    , condition.encounter_id as encounter_internal_id
    , 'encounter-diagnosis' as condition_category
    , condition.recorded_date as condition_recorded_datetime
    , coalesce(
          condition.onset_date
        , condition.recorded_date
      ) as condition_onset_datetime
    , condition.resolved_date as condition_abatement_datetime
    , condition.status as condition_clinical_status
    , case
        when lower(condition.normalized_code_type) = 'icd-10-cm'
            and len(condition.normalized_code) > 3
            then cast(substr(condition.normalized_code,1,3) as {{ dbt.type_string() }} )
                || '.'
                || cast(substr(condition.normalized_code,4) as {{ dbt.type_string() }} )
        else condition.normalized_code
      end as condition_code
    , case
        when lower(condition.normalized_code_type) = 'icd-10-cm' then 'ICD10'
        when lower(condition.normalized_code_type) = 'icd-9-cm' then 'ICD9'
        else condition.normalized_code_type
      end as condition_code_system
    , 'finished' as encounter_status
    , case
        when encounter.encounter_group = 'inpatient' then 'IMP'
        when encounter.encounter_group in ('outpatient', 'office based') then 'AMB'
        else 'other'
      end as encounter_class_code
    , encounter.encounter_start_date as encounter_start_datetime
    , encounter.encounter_end_date as encounter_end_datetime
    , condition.data_source
from {{ ref('fhir_preprocessing__stg_core__condition') }} as condition
    left outer join {{ ref('fhir_preprocessing__stg_core__encounter') }} as encounter
        on condition.encounter_id = encounter.encounter_id
where condition.normalized_code_type is not null
and condition.claim_id is null /* claim conditions are included in the EOB resource */
