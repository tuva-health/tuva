{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'annual wellness visit'
        , 'audiology visit'
        , 'home healthcare services'
        , 'nursing facility visit'
        , 'occupational therapy evaluation'
        , 'office visit'
        , 'physical therapy evaluation'
        , 'online assessments'
        , 'telephone visits'  
        , 'care services in long term residential facility'
        , 'discharge services nursing facility'
        , 'encounter inpatient'
        , 'ophthalmological services'
        , 'preventive care services established office visit, 18 and up'
        , 'preventive care services individual counseling'
        , 'preventive care services initial office visit, 18 and up'
    )

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
        , coalesce (
              normalized_code
            , source_code
          ) as code
    from {{ ref('quality_measures__stg_core__procedure') }}

)

, medical_claim as (
    
    select
          patient_id
        , claim_start_date
        , claim_end_date
        , hcpcs_code
    from {{ ref('quality_measures__stg_medical_claim') }}

)

, visits_encounters as (

    select patient_id
         , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
         , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from {{ ref('quality_measures__stg_core__encounter') }} encounter
    inner join {{ ref('quality_measures__int_cbe0101__performance_period') }} as pp
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
    from procedures
    inner join {{ ref('quality_measures__int_cbe0101__performance_period') }}  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join visit_codes
        on procedures.code = visit_codes.code

)

, claims_encounters as (
    
    select 
          patient_id
        , coalesce(claim_start_date,claim_end_date) as min_date
        , coalesce(claim_end_date,claim_start_date) as max_date
    from medical_claim
    inner join {{ ref('quality_measures__int_cbe0101__performance_period') }}  as pp 
        on coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
            and coalesce(claim_start_date,claim_end_date) <=  pp.performance_period_end
    inner join visit_codes
        on medical_claim.hcpcs_code = visit_codes.code

)

, all_encounters as (

    select *, 'v' as visit_enc, cast(null as {{ dbt.type_string() }}) as proc_enc, cast(null as {{ dbt.type_string() }}) as claim_enc
    from visits_encounters

    union all

    select *, cast(null as {{ dbt.type_string() }}) as visit_enc, 'p' as proc_enc, cast(null as {{ dbt.type_string() }}) as claim_enc
    from procedure_encounters

    union all
    
    select *, cast(null as {{ dbt.type_string() }}) as visit_enc, cast(null as {{ dbt.type_string() }}) as proc_enc, 'c' as claim_enc
    from claims_encounters

)

, encounters_by_patient as (

    select patient_id, min(min_date) min_date, max(max_date) max_date,
        concat(concat(
              coalesce(min(visit_enc),'')
            , coalesce(min(proc_enc),''))
            , coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by patient_id

)

, patients_with_age as (

    select
          p.patient_id
        , min_date
        , floor({{ datediff('birth_date', 'e.max_date', 'hour') }} / 8760.0) as age
        , max_date
        , qualifying_types
    from {{ ref('quality_measures__stg_core__patient') }} p
    inner join encounters_by_patient e
        on p.patient_id = e.patient_id
    where p.death_date is null

)

, falls_screening_code as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where code = '1100F'
    /* 
        Patient screened for future fall risk; documentation of two or more falls in the past year 
        or any fall with injury in the past year
    */
)

, qualifying_procedures as (

    select
          patient_id
        , procedure_date as evidence_date
    from procedures
    inner join falls_screening_code
        on procedures.code = falls_screening_code.code
            and procedures.code_type = falls_screening_code.code_system
            
)

, qualifying_claims as (

    select
          patient_id
        , coalesce(claim_end_date, claim_start_date) as evidence_date
    from medical_claim
    inner join falls_screening_code
        on medical_claim.hcpcs_code = falls_screening_code.code
            and lower(falls_screening_code.code_system) = 'hcpcs'

)

, qualifying_cares as (

    select
          patient_id
        , evidence_date
    from qualifying_procedures

    union all

    select
          patient_id
        , evidence_date
    from qualifying_claims

)

, qualifying_cares_past_year as (

    select
          patient_id
        , evidence_date
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
    from qualifying_cares
    inner join {{ ref('quality_measures__int_cbe0101__performance_period') }} pp
        on evidence_date between pp.performance_period_begin and pp.performance_period_end 
    /*  
        code 1100F is reported if there are two or more falls in the last year itself,
        so if it's reported in performance year, it indicates the falls in the last year
    */

)

, qualifying_patients as (

    select
          qualifying_cares_past_year.patient_id
        , patients_with_age.age
        , max_date as encounter_date
        , qualifying_cares_past_year.performance_period_begin
        , qualifying_cares_past_year.performance_period_end
        , qualifying_cares_past_year.measure_id
        , qualifying_cares_past_year.measure_name
        , qualifying_cares_past_year.measure_version
        , 1 as denominator_flag
    from qualifying_cares_past_year
    left join patients_with_age
        on qualifying_cares_past_year.patient_id = patients_with_age.patient_id
    where age >= 65

)


, add_data_types as (

    select distinct
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(encounter_date as date) as encounter_date
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      patient_id
    , age
    , encounter_date
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
