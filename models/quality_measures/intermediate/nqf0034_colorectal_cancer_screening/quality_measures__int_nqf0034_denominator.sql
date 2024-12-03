{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

/*
DENOMINATOR:
Patients 45-75 years of age with a visit during the measurement period
DENOMINATOR NOTE: To assess the age for exclusions, the patient’s age on the date of the encounter
should be used
*Signifies that this CPT Category I code is a non-covered service under the Medicare Part B Physician Fee
Schedule (PFS). These non-covered services should be counted in the denominator population for MIPS
CQMs.
Denominator Criteria (Eligible Cases):
Patients 45 to 75 years of age on date of encounter
AND
Patient encounter during the performance period (CPT or HCPCS): 99202, 99203, 99204, 99205,
99212, 99213, 99214, 99215, 99341, 99342, 99344, 99345, 99347, 99348, 99349, 99350, 99386*, 99387*,
99396*, 99397*, G0438, G0439
*/

with visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'office visit'
        , 'home healthcare services'
        , 'preventive care services established office visit, 18 and up'
        , 'preventive care services initial office visit, 18 and up'
        , 'annual wellness visit'
        , 'telephone visits'
        , 'online assessments'
    )

)

, visits_encounters as (

    select 
          person_id
        , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
        , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from {{ ref('quality_measures__stg_core__encounter') }} encounter
    inner join {{ ref('quality_measures__int_nqf0034__performance_period') }} as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
        and  coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    where encounter_type in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
    )

)

, procedure_encounters as (

    select 
          person_id
        , procedure_date as min_date
        , procedure_date as max_date
        from {{ ref('quality_measures__stg_core__procedure') }} procs
    inner join {{ ref('quality_measures__int_nqf0034__performance_period') }}  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join visit_codes
            on coalesce(procs.normalized_code,procs.source_code) = visit_codes.code

)

, claims_encounters as (

    select person_id
    , coalesce(claim_start_date,claim_end_date) as min_date
    , coalesce(claim_end_date,claim_start_date) as max_date
    from {{ref('quality_measures__stg_medical_claim')}} medical_claim
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}}  as pp on
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

    select person_id, min(min_date) min_date, max(max_date) max_date,
        concat(concat(
            coalesce(min(visit_enc),'')
            ,coalesce(min(proc_enc),''))
            ,coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by person_id

)

, patients_with_age as (

    select
          p.person_id
        , min_date
        , floor({{ datediff('birth_date', 'e.min_date', 'hour') }} / 8766.0)  as min_age
        , max_date
        ,floor({{ datediff('birth_date', 'e.max_date', 'hour') }} / 8766.0) as max_age
        , qualifying_types
    from {{ref('quality_measures__stg_core__patient')}} p
    inner join encounters_by_patient e
        on p.person_id = e.person_id
    where p.death_date is null -- ensures deceased patients are not included

)

select 
      person_id
    , min_age
    , max_age
    , qualifying_types
from patients_with_age
where max_age >= 45 and min_age <=  75