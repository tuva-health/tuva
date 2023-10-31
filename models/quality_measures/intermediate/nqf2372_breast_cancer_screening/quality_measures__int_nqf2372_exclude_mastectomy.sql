{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
    Women who had a bilateral mastectomy or who have a history of a bilateral
    mastectomy or for whom there is evidence of a right and a left
    unilateral mastectomy
*/

with denominator as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
    from {{ ref('quality_measures__int_nqf2372_denominator') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
          'Bilateral Mastectomy'
        , 'History of bilateral mastectomy'
        , 'Status Post Left Mastectomy'
        , 'Status Post Right Mastectomy'
        , 'Unilateral Mastectomy Left'
        , 'Unilateral Mastectomy Right'
        , 'Unilateral Mastectomy, Unspecified Laterality'
    )

)

, conditions as (

    select
          patient_id
        , recorded_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__condition') }}

)

, observations as (

    select
          patient_id
        , observation_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__observation') }}

)

, procedures as (

    select
          patient_id
        , procedure_date
        , coalesce (
              normalized_code_type
            , case
                when lower(source_code_type) = 'cpt' then 'hcpcs'
                when lower(source_code_type) = 'snomed' then 'snomed-ct'
                else lower(source_code_type)
              end
          ) as code_type
        , coalesce(
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, condition_exclusions as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , exclusion_codes.concept_name
    from conditions
         inner join exclusion_codes
             on conditions.code = exclusion_codes.code
             and conditions.code_type = exclusion_codes.code_system

)

, observation_exclusions as (

    select
          observations.patient_id
        , observations.observation_date
        , exclusion_codes.concept_name
    from observations
         inner join exclusion_codes
             on observations.code = exclusion_codes.code
             and observations.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.patient_id
        , procedures.procedure_date
        , exclusion_codes.concept_name
    from procedures
         inner join exclusion_codes
             on procedures.code = exclusion_codes.code
             and procedures.code_type = exclusion_codes.code_system

)

, all_mastectomy as (

    select
          denominator.patient_id
        , condition_exclusions.recorded_date as exclusion_date
        , condition_exclusions.concept_name as exclusion_reason
    from denominator
         inner join condition_exclusions
            on denominator.patient_id = condition_exclusions.patient_id

    union all

    select
          denominator.patient_id
        , observation_exclusions.observation_date as exclusion_date
        , observation_exclusions.concept_name as exclusion_reason
    from denominator
         inner join observation_exclusions
            on denominator.patient_id = observation_exclusions.patient_id

    union all

    select
          denominator.patient_id
        , procedure_exclusions.procedure_date as exclusion_date
        , procedure_exclusions.concept_name as exclusion_reason
    from denominator
         inner join procedure_exclusions
            on denominator.patient_id = procedure_exclusions.patient_id

)

/*
    Women who had a bilateral mastectomy or who have a history of a bilateral
    mastectomy
*/
, bilateral_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Bilateral Mastectomy'
        , 'History of bilateral mastectomy'
    )

)

, right_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Status Post Right Mastectomy'
        , 'Unilateral Mastectomy Right'
    )

)

, left_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
          'Status Post Left Mastectomy'
        , 'Unilateral Mastectomy Left'
    )

)

, unspecified_mastectomy as (

    select
          patient_id
        , exclusion_date
        , exclusion_reason
    from all_mastectomy
    where exclusion_reason in (
        'Unilateral Mastectomy, Unspecified Laterality'
    )

)

/*
    Women for whom there is evidence of a right AND a left unilateral mastectomy
    or unspecific mastectomies on different dates
*/
, unilateral_mastectomy as (

    select
          right_mastectomy.patient_id
        , right_mastectomy.exclusion_date
        , right_mastectomy.exclusion_reason
    from right_mastectomy
         inner join left_mastectomy
            on right_mastectomy.patient_id = left_mastectomy.patient_id

    union all

    select
          unspecified_mastectomy.patient_id
        , unspecified_mastectomy.exclusion_date
        , unspecified_mastectomy.exclusion_reason
    from unspecified_mastectomy
         inner join unspecified_mastectomy as self_join
            on unspecified_mastectomy.patient_id = self_join.patient_id
            and unspecified_mastectomy.exclusion_date <> self_join.exclusion_date

)

, unioned as (

    select * from bilateral_mastectomy
    union all
    select * from unilateral_mastectomy
)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from unioned