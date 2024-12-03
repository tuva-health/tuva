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
        , 'discharge services nursing facility'
        , 'hemodialysis'
        , 'home healthcare services'
        , 'nursing facility visit'
        , 'office visit'
        , 'outpatient consultation'
        , 'peritoneal dialysis'
        , 'preventive care services established office visit, 18 and up'
        , 'preventive care services group counseling'
        , 'preventive care services individual counseling'
        , 'preventive care services initial office visit, 18 and up'
        , 'preventive care services, initial office visit, 0 to 17'
        , 'preventive care, established office visit, 0 to 17'
        , 'online assessments'
        , 'patient provider interaction'
        , 'telephone visits'
    )

)

, visits_encounters as (

    select person_id
         , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as min_date
         , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as max_date
    from {{ ref('quality_measures__stg_core__encounter') }} encounter
    inner join {{ ref('quality_measures__int_nqf0041__performance_period') }} as pp
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
          person_id
        , procedure_date as min_date
        , procedure_date as max_date
    from {{ ref('quality_measures__stg_core__procedure') }} procedures
    inner join {{ ref('quality_measures__int_nqf0041__performance_period') }}  as pp
        on procedure_date between pp.performance_period_begin and  pp.performance_period_end
    inner join visit_codes
        on coalesce(procedures.normalized_code,procedures.source_code) = visit_codes.code

)

, claims_encounters as (
    
    select 
          person_id
        , coalesce(claim_start_date,claim_end_date) as min_date
        , coalesce(claim_end_date,claim_start_date) as max_date
    from {{ ref('quality_measures__stg_medical_claim') }} medical_claim
    inner join {{ ref('quality_measures__int_nqf0041__performance_period') }}  as pp on
        coalesce(claim_end_date,claim_start_date)  >=  pp.performance_period_begin
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

    select person_id, min(min_date) min_date, max(max_date) max_date,
        concat(concat(
              coalesce(min(visit_enc),'')
            , coalesce(min(proc_enc),''))
            , coalesce(min(claim_enc),'')
            ) as qualifying_types
    from all_encounters
    group by person_id

)

, patients_with_age as (

    select
          p.person_id
        , min_date
        , ({{ datediff('birth_date', 'e.max_date', 'hour') }} / 8760.0) as age_in_decimal_point
        , max_date
        , qualifying_types
    from {{ ref('quality_measures__stg_core__patient') }} p
    inner join encounters_by_patient e
        on p.person_id = e.person_id
    where p.death_date is null

)

, qualifying_patients as (

    select
        distinct
          patients_with_age.person_id
        , patients_with_age.age_in_decimal_point as age
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from patients_with_age
    cross join {{ ref('quality_measures__int_nqf0041__performance_period') }} pp
    where age_in_decimal_point >= 0.5 --filters patients aged 6 months or older

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , round(cast(age as {{ dbt.type_numeric() }}), 1) as age -- ensures age is seen in one decimal point
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      person_id
    , age
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
