{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__int_prep_conditions') }}

)

, observations as (

    select
          patient_id
        , observation_date
        , result
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__stg_core__observation') }}

)

, numeric_observations as (

    select
          patient_id
        , observation_date
        , cast(result as numeric) as result
        , code_type
        , code
        , data_source
    from observations
    where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}

)

, seed_clinical_concepts as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('hcc_suspecting__clinical_concepts') }}

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
    from {{ ref('hcc_suspecting__hcc_descriptions') }}

)

, billed_hccs as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}

)

, obstructive_sleep_apnea as (

     select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) = 'obstructive sleep apnea'

)

, diabetes as (

     select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) = 'diabetes'

)

, hypertension as (

     select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , seed_clinical_concepts.concept_name
    from conditions
        inner join seed_clinical_concepts
            on conditions.code_type = seed_clinical_concepts.code_system
            and conditions.code = seed_clinical_concepts.code
    where lower(seed_clinical_concepts.concept_name) = 'essential hypertension'

)

/* BEGIN HCC 48 logic */
, bmi_over_30_with_osa as (

    select
          numeric_observations.patient_id
        , numeric_observations.data_source
        , numeric_observations.observation_date
        , numeric_observations.result as observation_result
        , obstructive_sleep_apnea.code as condition_code
        , obstructive_sleep_apnea.recorded_date as condition_date
        , obstructive_sleep_apnea.concept_name as condition_concept_name
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from numeric_observations
        inner join seed_clinical_concepts
            on numeric_observations.code_type = seed_clinical_concepts.code_system
            and numeric_observations.code = seed_clinical_concepts.code
        inner join obstructive_sleep_apnea
            on numeric_observations.patient_id = obstructive_sleep_apnea.patient_id
            /* ensure bmi and condition overlaps in the same year */
            and extract(year from numeric_observations.observation_date) = extract(year from obstructive_sleep_apnea.recorded_date)
        inner join seed_hcc_descriptions
            on hcc_code = '48'
    where lower(seed_clinical_concepts.concept_name) = 'bmi'
    and result >= 30

)

, bmi_over_35_with_diabetes as (

    select
          numeric_observations.patient_id
        , numeric_observations.data_source
        , numeric_observations.observation_date
        , numeric_observations.result as observation_result
        , diabetes.code as condition_code
        , diabetes.recorded_date as condition_date
        , diabetes.concept_name as condition_concept_name
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from numeric_observations
        inner join seed_clinical_concepts
            on numeric_observations.code_type = seed_clinical_concepts.code_system
            and numeric_observations.code = seed_clinical_concepts.code
        inner join diabetes
            on numeric_observations.patient_id = diabetes.patient_id
            /* ensure bmi and condition overlaps in the same year */
            and extract(year from numeric_observations.observation_date) = extract(year from diabetes.recorded_date)
        inner join seed_hcc_descriptions
            on hcc_code = '48'
    where lower(seed_clinical_concepts.concept_name) = 'bmi'
    and result >= 35

)

, bmi_over_35_with_hypertension as (

    select
          numeric_observations.patient_id
        , numeric_observations.data_source
        , numeric_observations.observation_date
        , numeric_observations.result as observation_result
        , hypertension.code as condition_code
        , hypertension.recorded_date as condition_date
        , hypertension.concept_name as condition_concept_name
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from numeric_observations
        inner join seed_clinical_concepts
            on numeric_observations.code_type = seed_clinical_concepts.code_system
            and numeric_observations.code = seed_clinical_concepts.code
        inner join hypertension
            on numeric_observations.patient_id = hypertension.patient_id
            /* ensure bmi and condition overlaps in the same year */
            and extract(year from numeric_observations.observation_date) = extract(year from hypertension.recorded_date)
        inner join seed_hcc_descriptions
            on hcc_code = '48'
    where lower(seed_clinical_concepts.concept_name) = 'bmi'
    and result >= 35

)

, bmi_over_40 as (

    select
          numeric_observations.patient_id
        , numeric_observations.data_source
        , numeric_observations.observation_date
        , numeric_observations.result as observation_result
        , cast(null as {{ dbt.type_string() }}) as condition_code
        , cast(null as date) as condition_date
        , cast(null as {{ dbt.type_string() }}) as condition_concept_name
        , seed_hcc_descriptions.hcc_code
        , seed_hcc_descriptions.hcc_description
    from numeric_observations
        inner join seed_clinical_concepts
            on numeric_observations.code_type = seed_clinical_concepts.code_system
            and numeric_observations.code = seed_clinical_concepts.code
        inner join seed_hcc_descriptions
            on hcc_code = '48'
    where lower(seed_clinical_concepts.concept_name) = 'bmi'
    and result >= 40

)

, hcc_48_suspect as (

    select * from bmi_over_30_with_osa
    union all
    select * from bmi_over_35_with_diabetes
    union all
    select * from bmi_over_35_with_hypertension
    union all
    select * from bmi_over_40

)
/* END HCC 48 logic */

, unioned as (

    select * from hcc_48_suspect

)

, add_billed_flag as (

    select
          unioned.patient_id
        , unioned.data_source
        , unioned.observation_date
        , unioned.observation_result
        , unioned.condition_code
        , unioned.condition_date
        , unioned.condition_concept_name
        , unioned.hcc_code
        , unioned.hcc_description
        , billed_hccs.current_year_billed
    from unioned
        left join billed_hccs
            on unioned.patient_id = billed_hccs.patient_id
            and unioned.data_source = billed_hccs.data_source
            and unioned.hcc_code = billed_hccs.hcc_code

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(observation_date as date) as observation_date
        , cast(observation_result as {{ dbt.type_string() }}) as observation_result
        , cast(condition_code as {{ dbt.type_string() }}) as condition_code
        , cast(condition_date as date) as condition_date
        , cast(condition_concept_name as {{ dbt.type_string() }}) as condition_concept_name
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(current_year_billed as boolean) as current_year_billed
    from add_billed_flag

)

select
      patient_id
    , data_source
    , observation_date
    , observation_result
    , condition_code
    , condition_date
    , condition_concept_name
    , hcc_code
    , hcc_description
    , current_year_billed
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types