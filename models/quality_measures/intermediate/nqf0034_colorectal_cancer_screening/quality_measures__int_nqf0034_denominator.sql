{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
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

with  visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
          'Office Visit'
        , 'Home Healthcare Services'
        , 'Preventive Care Services Established Office Visit, 18 and Up'
        , 'Preventive Care Services Initial Office Visit, 18 and Up'
        , 'Annual Wellness Visit'
        , 'Telephone Visits'
        , 'Online Assessments'
    )

), visits_encounters as (
    select PATIENT_ID
         , coalesce(ENCOUNTER.ENCOUNTER_START_DATE,ENCOUNTER.ENCOUNTER_END_DATE) as min_date
         , coalesce(ENCOUNTER.ENCOUNTER_END_DATE,ENCOUNTER.ENCOUNTER_START_DATE) as max_date
    From {{ref('quality_measures__stg_core__encounter')}} encounter
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}} as pp
        on coalesce(ENCOUNTER.ENCOUNTER_END_DATE,ENCOUNTER.ENCOUNTER_START_DATE) >= pp.performance_period_begin
        and  coalesce(ENCOUNTER.ENCOUNTER_START_DATE,ENCOUNTER.ENCOUNTER_END_DATE) <= pp.performance_period_end
    where ENCOUNTER_TYPE in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
        )


      )

,procedure_encounters as (
    select patient_id, PROCEDURE_DATE as min_date, PROCEDURE_DATE as max_date
    from {{ref('quality_measures__stg_core__procedure')}} proc
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}}  as pp
        on PROCEDURE_DATE between pp.performance_period_begin and  pp.performance_period_end
    inner join  visit_codes
        on coalesce(proc.normalized_code,proc.source_code) = visit_codes.code


)
,
claims_encounters as (
    select PATIENT_ID
    , coalesce(CLAIM_START_DATE,CLAIM_END_DATE) as min_date
    , coalesce(CLAIM_END_DATE,CLAIM_START_DATE) as max_date
    from {{ref('quality_measures__stg_medical_claim')}} medical_claim
    inner join {{ref('quality_measures__int_nqf0034__performance_period')}}  as pp on
        coalesce(CLAIM_END_DATE,CLAIM_START_DATE)  >=  pp.performance_period_begin
         and coalesce(CLAIM_START_DATE,CLAIM_END_DATE) <=  pp.performance_period_end
    inner join  visit_codes
        on medical_claim.hcpcs_code= visit_codes.code


)

,all_encounters as (
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
          p.PATIENT_ID
        , min_date
        , floor({{ datediff('birth_date', 'e.min_date', 'hour') }} / 8766.0)  as min_age
        , max_date
        ,floor({{ datediff('birth_date', 'e.max_date', 'hour') }} / 8766.0) as max_age
        , qualifying_types
    from {{ref('quality_measures__stg_core__patient')}} p
    inner join encounters_by_patient e
        on p.PATIENT_ID = e.PATIENT_ID
    where p.BIRTH_DATE is not null

)

select PATIENT_ID,
       min_age,
       max_age,
       qualifying_types
From patients_with_age
where max_age >= 45 and min_age <=  75