{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

{%- set performance_period_begin -%}
(
  select 
    performance_period_begin
  from {{ ref('quality_measures__int_cqm438__performance_period') }}

)
{%- endset -%}

{%- set performance_period_end -%}
(
  select 
    performance_period_end
  from {{ ref('quality_measures__int_cqm438__performance_period') }}

)
{%- endset -%}

with exclusion_codes as (

    select
          code
        , case code_system
            when 'SNOMEDCT' then 'snomed-ct'
            when 'ICD9CM' then 'icd-9-cm'
            when 'ICD10CM' then 'icd-10-cm'
            when 'CPT' then 'hcpcs'
            when 'ICD10PCS' then 'icd-10-pcs'
          else lower(code_system) end as code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
            'rhabdomyolysis'
          , 'breastfeeding'
          , 'liver disease'
          , 'hepatitis a'
          , 'hepatitis b'
          , 'documentation of medical reason for no statin therapy'
          , 'statin allergen'
          , 'end stage renal disease'
          , 'statin associated muscle symptoms'
          , 'medical reason'
    )

)

, valid_hospice_palliative as (

  select
      person_id
    , exclusion_date
    , exclusion_reason
    , exclusion_type
  from {{ ref('quality_measures__int_shared_exclusions_hospice_palliative') }}
  where exclusion_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, conditions as (

    select
          person_id
        , claim_id
        , recorded_date
        , coalesce(
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
    where recorded_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, medical_claim as (

    select
          person_id
        , claim_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
        , place_of_service_code
    from {{ ref('quality_measures__stg_medical_claim') }}
    where coalesce(claim_end_date, claim_start_date) between {{ performance_period_begin }} and {{ performance_period_end }}

)

, observations as (

    select
          person_id
        , observation_date
        , coalesce(
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
    where observation_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, procedures as (

    select
          person_id
        , procedure_date
        , coalesce(
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
    where procedure_date between {{ performance_period_begin }} and {{ performance_period_end }}

)

, medications as (

    select
        person_id
      , coalesce(prescribing_date, dispensing_date) as exclusion_date
      , source_code
      , source_code_type
    from {{ ref('quality_measures__stg_core__medication') }}

)

, pharmacy_claims as (

    select
        person_id
      , dispensing_date
      , ndc_code
    from {{ ref('quality_measures__stg_pharmacy_claim') }}

)

, condition_exclusions as (

    select
          conditions.person_id
        , conditions.claim_id
        , conditions.recorded_date
        , exclusion_codes.concept_name as concept_name
    from conditions
         inner join exclusion_codes
            on conditions.code = exclusion_codes.code
            and conditions.code_type = exclusion_codes.code_system

)

, med_claim_exclusions as (

    select
          medical_claim.person_id
        , medical_claim.claim_id
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.hcpcs_code
        , exclusion_codes.concept_name as concept_name
    from medical_claim
         inner join exclusion_codes
            on medical_claim.hcpcs_code = exclusion_codes.code
    where exclusion_codes.code_system = 'hcpcs'

)

, observation_exclusions as (

    select
          observations.person_id
        , observations.observation_date
        , exclusion_codes.concept_name as concept_name
    from observations
    inner join exclusion_codes
        on observations.code = exclusion_codes.code
        and observations.code_type = exclusion_codes.code_system

)

, procedure_exclusions as (

    select
          procedures.person_id
        , procedures.procedure_date
        , exclusion_codes.concept_name as concept_name
    from procedures
    inner join exclusion_codes
        on procedures.code = exclusion_codes.code
          and procedures.code_type = exclusion_codes.code_system

)

, medication_exclusions as (

    select
          medications.person_id
        , medications.exclusion_date
        , exclusion_codes.concept_name as concept_name
    from medications
    inner join exclusion_codes
        on medications.source_code = exclusion_codes.code
          and medications.source_code_type = exclusion_codes.code_system

)

, pharmacy_claims_exclusions as (

    select
          pharmacy_claims.person_id
        , pharmacy_claims.dispensing_date
        , exclusion_codes.concept_name as concept_name
    from pharmacy_claims
    inner join exclusion_codes
        on pharmacy_claims.ndc_code = exclusion_codes.code
        and lower(exclusion_codes.code_system) = 'ndc'

)

, patients_with_exclusions as (

    select
          person_id
        , recorded_date as exclusion_date
        , concept_name as exclusion_reason
    from condition_exclusions

    union all

    select
          person_id
        , coalesce(claim_end_date, claim_start_date) as exclusion_date
        , concept_name as exclusion_reason
    from med_claim_exclusions

    union all

    select
          person_id
        , observation_date as exclusion_date
        , concept_name as exclusion_reason
    from observation_exclusions

    union all

    select
          person_id
        , procedure_date as exclusion_date
        , concept_name as exclusion_reason
    from procedure_exclusions

    union all

    select
          person_id
        , exclusion_date
        , concept_name as exclusion_reason
    from medication_exclusions

    union all

    select
          person_id
        , dispensing_date as exclusion_date
        , concept_name as exclusion_reason
    from pharmacy_claims_exclusions

    union all

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from valid_hospice_palliative

)

, valid_exclusions as (

  select
        patients_with_exclusions.person_id
      , patients_with_exclusions.exclusion_date
      , patients_with_exclusions.exclusion_reason
  from patients_with_exclusions
  inner join {{ ref('quality_measures__int_cqm438_denominator') }} as denominator
      on patients_with_exclusions.person_id = denominator.person_id

)

, add_data_types as (

    select
        distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(exclusion_date as date) as exclusion_date
        , cast(exclusion_reason as {{ dbt.type_string() }}) as exclusion_reason
        , cast(1 as integer) as exclusion_flag
    from valid_exclusions

)

select
      person_id
    , exclusion_date
    , exclusion_reason
    , exclusion_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
