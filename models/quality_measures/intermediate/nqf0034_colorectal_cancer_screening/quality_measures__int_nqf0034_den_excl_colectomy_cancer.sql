{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
DENOMINATOR EXCLUSIONS:
Patients with a diagnosis or past history of total colectomy or colorectal cancer: G9711
*/



{%- set performance_period_end = var('quality_measures_period_end') -%}

{%- set performance_period_begin -%}
{{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="'"~performance_period_end~"'") }}
{%- endset -%}



with visits_encounters as (
    select PATIENT_ID, ENCOUNTER_START_DATE as min_date, ENCOUNTER_END_DATE as max_date
    From TUVA_TEST.core.ENCOUNTER
    inner join {{ref('quality_measures__int_nqf0034_performance_period')}} as pp on 1=1
    where ENCOUNTER_TYPE in (
          'home health'
        , 'office visit'
        , 'outpatient'
        , 'outpatient rehabilitation'
        , 'telehealth'
        )
    and coalesce(ENCOUNTER.ENCOUNTER_END_DATE,ENCOUNTER.ENCOUNTER_START_DATE) >= pp.performance_period_begin
    and  coalesce(ENCOUNTER.ENCOUNTER_START_DATE,ENCOUNTER.ENCOUNTER_END_DATE) <= pp.performance_period_end
 --coalesces are for null end (or start) dates

      ) -- Todo: is this right?  I have any part of the encoutner overlapping, but should it need to be entirely inthe reporting period?


,
proc_codes as (select $1 as cpt
             From (values ('99202'), ('99203'), ('99204'), ('99205')
                        , ('99212'), ('99213'), ('99214'), ('99215')
                        , ('99341'), ('99342'), ('99344'), ('99345')
                        , ('99347'), ('99348'), ('99349'), ('99350')
                        , ('99386'), ('99387'), ('99396'), ('99397')
                        , ('G0438'), ('G0439')
                 )
             )

,procedure_encounters as (
    select patient_id, PROCEDURE_DATE as min_date, PROCEDURE_DATE as max_date
    from TUVA_TEST.CORE.PROCEDURE
    inner join {{ref('quality_measures__int_nqf0034_performance_period')}}  as pp on 1=1
    inner join  proc_codes  -- todo: should it be this (proc codes from measure definition), or should it be from the value set codes?
        on procedure.code = proc_codes.cpt
 where PROCEDURE_DATE between pp.performance_period_begin and  pp.performance_period_end

)
,
claims_encounters as (
    select PATIENT_ID, CLAIM_START_DATE as min_date,CLAIM_END_DATE as max_date
    from tuva_test.core.MEDICAL_CLAIM
    inner join {{ref('quality_measures__int_nqf0034_performance_period')}}  as pp on 1=1
    inner join proc_codes
        on MEDICAL_CLAIM.HCPCS_CODE = proc_codes.cpt
            where coalesce(MEDICAL_CLAIM.CLAIM_END_DATE,MEDICAL_CLAIM.CLAIM_START_DATE) >=  pp.performance_period_begin
         and coalesce(MEDICAL_CLAIM.CLAIM_END_DATE,MEDICAL_CLAIM.CLAIM_END_DATE) <=  pp.performance_period_end

)

,all_encounters as (
    select *, 'v' as visit_enc,cast(null as varchar) as proc_enc, cast(null as varchar) as claim_enc
    from visits_encounters
    union all
    select *, cast(null as varchar) as visit_enc, 'p' as proc_enc, cast(null as varchar) as claim_enc
    from procedure_encounters
    union all
    select *, cast(null as varchar) as visit_enc,cast(null as varchar) as proc_enc, 'c' as claim_enc
    from claims_encounters
)

, encounters_by_patient as (
    select patient_id,min(min_date) min_date, max(max_date) max_date,
        concat(
            coalesce(min(visit_enc),'')
            ,coalesce(min(proc_enc),'')
            ,coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by patient_id
)

, patients_with_age as (
    select
          p.PATIENT_ID
        , min_date
        , datediff('year',p.BIRTH_DATE,e.min_date) as min_age
        , max_date
        , datediff('year',p.BIRTH_DATE,e.max_date) as max_age
    from TUVA_TEST.core.PATIENT p
    inner join encounters_by_patient e
        on p.PATIENT_ID = e.PATIENT_ID
    where p.BIRTH_DATE is not null

)

select PATIENT_ID,
       min_age,
       max_age,
       case when max_age >= 45 and min_age <=  75 then 1 else 0 end as age_qualifies
From patients_with_age