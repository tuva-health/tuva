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

, value_sets as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('hcc_suspecting__value_sets') }}

)

, billed_hccs as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}

)

/* BEGIN HCC 37 logic */
, ckd_stage_1_or_2 as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , value_sets.concept_name
        , row_number() over (
            partition by
                  conditions.patient_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join value_sets
            on conditions.code_type = value_sets.code_system
            and conditions.code = value_sets.code
    where lower(value_sets.concept_name) in (
          'chronic kidney disease, stage 1'
        , 'chronic kidney disease, stage 2'
    )

)

, ckd_stage_1_or_2_dedupe as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from ckd_stage_1_or_2
    where row_num = 1

)

, diabetes as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , conditions.data_source
        , value_sets.concept_name
        , row_number() over (
            partition by
                  conditions.patient_id
                , conditions.data_source
            order by
                  conditions.recorded_date desc
                , conditions.code desc
          ) as row_num
    from conditions
        inner join value_sets
            on conditions.code_type = value_sets.code_system
            and conditions.code = value_sets.code
    where lower(value_sets.concept_name) = 'diabetes'
)

, diabetes_dedupe as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
        , concept_name
    from diabetes
    where row_num = 1

)

, hcc_37_suspect as (

    select
          diabetes_dedupe.patient_id
        , diabetes_dedupe.data_source
        , '37' as hcc_code
        , 'Diabetes with Chronic Complications Logic' as hcc_description
        , 'Comorbidity suspect' as reason
        , diabetes_dedupe.concept_name
            || ' and '
            || ckd_stage_1_or_2_dedupe.concept_name
            as contributing_factor
        , diabetes_dedupe.recorded_date
    from diabetes_dedupe
        inner join ckd_stage_1_or_2_dedupe
            on diabetes_dedupe.patient_id = ckd_stage_1_or_2_dedupe.patient_id
            /* ensure conditions overlap in the same year */
            and extract(year from diabetes_dedupe.recorded_date) = extract(year from ckd_stage_1_or_2_dedupe.recorded_date)

)
/* END HCC 37 logic */

, unioned as (

    select * from hcc_37_suspect

)

, add_billed_flag as (

    select
          unioned.patient_id
        , unioned.data_source
        , unioned.hcc_code
        , unioned.hcc_description
        , unioned.reason
        , unioned.contributing_factor
        , unioned.recorded_date
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
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
        , cast(recorded_date as date) as condition_date
        , cast(current_year_billed as boolean) as current_year_billed
    from add_billed_flag

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , condition_date
    , current_year_billed
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types