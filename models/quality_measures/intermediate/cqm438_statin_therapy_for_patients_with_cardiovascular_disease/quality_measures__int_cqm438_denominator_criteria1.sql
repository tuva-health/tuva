{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with visit_codes as (

    select
          value_sets.code
        , value_sets.code_system
    from {{ ref('quality_measures__value_sets') }} value_sets
    inner join {{ ref('quality_measures__concepts') }} concepts
        on value_sets.concept_name = concepts.concept_name
            and concepts.measure_id = 'CQM438'

)

, visits_encounters as (

    select patient_id
         , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
         , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from {{ref('quality_measures__stg_core__encounter')}} encounter
    inner join {{ref('quality_measures__int_cqm438__performance_period')}} as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
        and  coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    where lower(encounter_type) in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
     )

)

, procedure_encounters as (

    select 
          patient_id
        , procedure_date as min_date
        , procedure_date as max_date
    from {{ref('quality_measures__stg_core__procedure')}} proc
    inner join {{ref('quality_measures__int_cqm438__performance_period')}}  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join visit_codes
        on coalesce(proc.normalized_code,proc.source_code) = visit_codes.code

)

, claims_encounters as (
    
    select patient_id
    , coalesce(claim_start_date,claim_end_date) as min_date
    , coalesce(claim_end_date,claim_start_date) as max_date
    from {{ref('quality_measures__stg_medical_claim')}} medical_claim
    inner join {{ref('quality_measures__int_cqm438__performance_period')}}  as pp on
        coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
         and coalesce(claim_start_date,claim_end_date) <=  pp.performance_period_end
    inner join  visit_codes
        on medical_claim.hcpcs_code= visit_codes.code

)

, all_encounters as (

    select *, 'v' as visit_enc,cast(null as {{ dbt.type_string() }}) as proc_enc, cast(null as {{ dbt.type_string() }}) as claim_enc
    from visits_encounters

    union all

    select *, cast(null as {{ dbt.type_string() }}) as visit_enc, 'p' as proc_enc, cast(null as {{ dbt.type_string() }}) as claim_enc
    from procedure_encounters

    union all
    
    select *, cast(null as {{ dbt.type_string() }}) as visit_enc,cast(null as {{ dbt.type_string() }}) as proc_enc, 'c' as claim_enc
    from claims_encounters

)

, encounters_by_patient as (

    select patient_id,min(min_date) min_date, max(max_date) max_date,
        concat(concat(
            coalesce(min(visit_enc),'')
            ,coalesce(min(proc_enc),''))
            ,coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by patient_id

)

, ascvd_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
              'atherosclerosis and peripheral arterial disease'
            , 'myocardial infarction'
            , 'pci'
            , 'stable and unstable angina'
            , 'atherosclerosis and peripheral arterial disease'
            , 'cabg or pci procedure'
            , 'cabg surgeries'
            , 'cerebrovascular disease stroke or tia'
            , 'ischemic heart disease or related diagnoses'
        )

)

, conditions as (

    select
          patient_id
        , claim_id
        , encounter_id
        , recorded_date
        , source_code
        , source_code_type
        , normalized_code
        , normalized_code_type
    from {{ ref('quality_measures__stg_core__condition')}}

)

, ascvd_conditions as (

    select
          conditions.patient_id
        , conditions.recorded_date as evidence_date
    from conditions
    inner join ascvd_codes
        on conditions.source_code_type = ascvd_codes.code_system
            and conditions.source_code = ascvd_codes.code

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

, ascvd_procedures as (

    select
          procedures.patient_id
        , procedures.procedure_date as evidence_date
    from procedures
         inner join ascvd_codes
             on procedures.code = ascvd_codes.code
             and procedures.code_type = ascvd_codes.code_system

)

, historical_ascvd as (

    select
          ascvd_conditions.patient_id
        , ascvd_conditions.evidence_date
    from ascvd_conditions

    union all

    select
          ascvd_procedures.patient_id
        , ascvd_procedures.evidence_date
    from ascvd_procedures

)

, patients_with_ascvd as (

    select
          patient_id
        , evidence_date
    from historical_ascvd
    cross join {{ref('quality_measures__int_cqm438__performance_period')}} pp
    where evidence_date <= pp.performance_period_end

)

, patients_with_age as (

    select
          p.patient_id
        , min_date
        , floor({{ datediff('birth_date', 'e.min_date', 'hour') }} / 8760.0)  as min_age
        , max_date
        , floor({{ datediff('birth_date', 'e.max_date', 'hour') }} / 8760.0) as max_age
        , qualifying_types
    from {{ref('quality_measures__stg_core__patient')}} p
    inner join encounters_by_patient e
        on p.patient_id = e.patient_id
    where p.death_date is null

)

, qualifying_patients as (

    select
        distinct
          patients_with_ascvd.patient_id
        , patients_with_age.max_age as age
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from patients_with_ascvd
    left join patients_with_age
        on patients_with_ascvd.patient_id = patients_with_age.patient_id
    cross join {{ref('quality_measures__int_cqm438__performance_period')}} pp
    where patients_with_age.max_age is not null

)

select 
      patient_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
from qualifying_patients
