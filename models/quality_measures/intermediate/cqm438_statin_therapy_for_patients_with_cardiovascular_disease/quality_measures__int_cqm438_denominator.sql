{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with patients_with_ascvd as (

    select 
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , 1 as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria1') }}

)

, patients_with_cholesterol as (

    select 
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , 2 as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria2') }}

)

, patients_with_diabetes as (

    select 
          patient_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , 3 as criteria
    from {{ ref('quality_measures__int_cqm438_denominator_criteria3') }}

)


, visit_codes as (

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
    inner join visit_codes
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

, patients_with_age as (

    select
          p.patient_id
        , floor({{ datediff('birth_date', 'performance_period_begin', 'hour') }} / 8760.0)  as age
    from {{ref('quality_measures__stg_core__patient')}} p
    inner join encounters_by_patient e
        on p.patient_id = e.patient_id
            and p.death_date is null
    cross join {{ref('quality_measures__int_cqm438__performance_period')}}

)

, qualifying_patients_from_criteria1 as (

    select
        distinct
          patients_with_ascvd.patient_id
        , patients_with_age.age as age
        , patients_with_ascvd.performance_period_begin
        , patients_with_ascvd.performance_period_end
        , patients_with_ascvd.measure_id
        , patients_with_ascvd.measure_name
        , patients_with_ascvd.measure_version
        , 1 as denominator_flag
    from patients_with_ascvd
    left join patients_with_age
    on patients_with_ascvd.patient_id = patients_with_age.patient_id
    where age is not null

)

, qualifying_patients_from_criteria2 as (

    select
        distinct
          patients_with_cholesterol.patient_id
        , patients_with_age.age as age
        , patients_with_cholesterol.performance_period_begin
        , patients_with_cholesterol.performance_period_end
        , patients_with_cholesterol.measure_id
        , patients_with_cholesterol.measure_name
        , patients_with_cholesterol.measure_version
        , 1 as denominator_flag
    from patients_with_cholesterol
    left join patients_with_age
    where age between 20 and 75

)

, qualifying_patients_from_criteria3 as (

    select
        distinct
          patients_with_diabetes.patient_id
        , patients_with_age.age as age
        , patients_with_diabetes.performance_period_begin
        , patients_with_diabetes.performance_period_end
        , patients_with_diabetes.measure_id
        , patients_with_diabetes.measure_name
        , patients_with_diabetes.measure_version
        , 1 as denominator_flag
    from patients_with_diabetes
    left join patients_with_age
    on patients_with_diabetes.patient_id = patients_with_age.patient_id
    where age between 40 and 75

)

, final_denominator as (
    
    select *
    from qualifying_patients_from_criteria1

    union

    select *
    from qualifying_patients_from_criteria2
    
    union

    select *
    from qualifying_patients_from_criteria3

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from final_denominator

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
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
