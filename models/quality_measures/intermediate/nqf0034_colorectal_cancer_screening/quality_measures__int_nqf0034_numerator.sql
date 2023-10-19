{{ config(
     enabled = true
   )
}}

with denominator as (

    select
          patient_id
--         , performance_period_begin
--         , performance_period_end
--         , measure_id
--         , measure_name
--         , measure_version
    from {{ ref('quality_measures__int_nqf0034_denominator') }}

)
, screening_codes as
(
    select
          code
        , Descriptor
        , case CodeSystemName
            when 'SNOMEDCT' then 'snomed-ct'
            when 'HCPCS Level II' then 'hcpcs'
            when 'CPT' then 'cpt'
            when 'LOINC' then 'loinc'
          else lower(codesystemname) end as code_system
        , ValueSetName as concept_name
    From {{ref('quality_measures__value_set_codes')}}
    where ValueSetName in  (
        'Fecal Occult Blood Test (FOBT)' -- mp
        ,'Flexible Sigmoidoscopy' --mp+4
        ,'Colonoscopy' -- mp+9
        ,'CT Colonography' -- mp+4
        ,'FIT DNA' -- mp+2
    )
)
, screening_periods  as (
    select *,
        case concept_name
            when 'Fecal Occult Blood Test (FOBT)' then pp.performance_period_begin --mp
            when 'Flexible Sigmoidoscopy' then pp.performance_period_begin_4yp --mp+4
            when 'Colonoscopy' then pp.performance_period_begin_9yp -- mp+9
            when 'CT Colonography' then pp.performance_period_begin_4yp -- mp+4
            when 'FIT DNA' then pp.performance_period_begin_2yp -- mp+2
        else pp.performance_period_begin end as effective_performance_period_begin

    from screening_codes
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} pp
        on 1 = 1
    )


, medical_claim as (

    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
    from {{ ref('quality_measures__stg_medical_claim') }}

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
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__observation') }}

)

, procedures as (

    select
          patient_id
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

)

, labs as (
    select  patient_id
    , result_date
    , collection_date
    , source_code_type
    , source_code
    , normalized_code_type
    , normalized_code
    from {{ ref('quality_measures__stg_core__lab_result')}}
    )


, qualifying_claims as (

    select
          medical_claim.patient_id
        , coalesce( medical_claim.claim_start_date, medical_claim.claim_end_date) as claim_date
    , screening_codes.descriptor
    from medical_claim
    inner join screening_periods
        on medical_claim.claim_start_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
        or medical_claim.claim_end_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
    inner join screening_codes
            on medical_claim.hcpcs_code = screening_codes.code
    where screening_codes.code_system in ('hcpcs', 'cpt' )

)

, qualifying_observations as (

    select
          observations.patient_id
        , observations.observation_date
    , screening_codes.descriptor
    from observations
    inner join screening_periods
        on observations.observation_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
     inner join screening_codes
         on observations.code = screening_codes.code
         and observations.code_type = screening_codes.code_system
)

, qualifying_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date
    , screening_codes.descriptor
    from procedures
    inner join screening_periods
        on procedures.procedure_date between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
     inner join screening_codes
         on procedures.code = screening_codes.code
         and procedures.code_type = screening_codes.code_system

)

,qualifying_labs as (
    select
      patient_id
    , coalesce(collection_date,result_date) as lab_date
    , screening_codes.descriptor
    from labs
    inner join screening_periods
        on coalesce(labs.collection_date, labs.result_date) between screening_periods.effective_performance_period_begin and screening_periods.performance_period_end
    inner join  screening_codes
      on ( labs.normalized_code = screening_codes.code
       and labs.normalized_code_type = screening_codes.code_system )
      or ( labs.source_code = screening_codes.code
       and labs.source_code_type = screening_codes.code_system )
    )

,qualifying_events as (
    select
          patient_id
        , claim_date as evidence_date
        , descriptor as evidence
    from qualifying_claims

    union

    select
          patient_id
        , observation_date as evidence_date
        , descriptor as evidence
    from qualifying_observations

    union

    select
          patient_id
        , procedure_date as evidence_date
        , descriptor as evidence
    from qualifying_procedures

    union

    select
          patient_id
        , lab_date as evidence_date
        , descriptor as evidence
    from qualifying_labs

    )

select
    cast (patient_id as varchar) patient_id
    ,cast( evidence_date as date) as evidence_date
    ,cast( evidence as varchar) as evidence

from qualifying_events


