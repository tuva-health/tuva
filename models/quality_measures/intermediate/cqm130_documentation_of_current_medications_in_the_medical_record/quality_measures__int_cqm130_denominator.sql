{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
     | as_bool
   )
}}

with visit_codes as (

    select
          code
        , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
            'encounter to document medications'
    )

)

, visits_encounters as (

    select 
          person_id
        , coalesce(encounter.encounter_start_date,encounter.encounter_end_date) as procedure_encounter_date -- alias only to enable union later
        , coalesce(encounter.encounter_end_date,encounter.encounter_start_date) as claims_encounter_date -- alias only to enable union later
    from {{ ref('quality_measures__stg_core__encounter') }} encounter
    inner join {{ ref('quality_measures__int_cqm130__performance_period') }} as pp
        on coalesce(encounter.encounter_end_date,encounter.encounter_start_date) >= pp.performance_period_begin
            and coalesce(encounter.encounter_start_date,encounter.encounter_end_date) <= pp.performance_period_end
    where lower(encounter_type) in (
          'home health'
        , 'telehealth'
    )
    
    or lower(encounter_group) in (
          'office based'
        , 'outpatient'
    )

)

, procedure_encounters as (

    select 
          person_id
        , procedure_date as procedure_encounter_date
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as claims_encounter_date
    from {{ ref('quality_measures__stg_core__procedure') }} procs
    inner join {{ ref('quality_measures__int_cqm130__performance_period') }} as pp
        on procedure_date between pp.performance_period_begin and pp.performance_period_end
    inner join visit_codes
        on coalesce(procs.normalized_code,procs.source_code) = visit_codes.code

)

, claims_encounters as (
    
    select 
          person_id
        , {{ try_to_cast_date('null', 'YYYY-MM-DD') }} as procedure_encounter_date
        , coalesce(claim_end_date,claim_start_date) as claims_encounter_date
    from {{ ref('quality_measures__stg_medical_claim') }} medical_claim
    inner join {{ ref('quality_measures__int_cqm130__performance_period') }} as pp 
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

, multiple_encounters_by_patient as (

    select
          person_id
        , procedure_encounter_date
        , claims_encounter_date
        , case when procedure_encounter_date >= claims_encounter_date
                then procedure_encounter_date
            else claims_encounter_date
          end as max_encounter_date
        , {{ concat_custom([
              "coalesce(min(visit_enc), '')"
            , "coalesce(min(proc_enc), '')"
            , "coalesce(min(claim_enc), '')"
        ]) }} as qualifying_types
    from all_encounters
    group by person_id, procedure_encounter_date, claims_encounter_date

)

, max_encounter_dates_by_patient as (

	select
		  person_id
		, max(max_encounter_date) as max_encounter_date
	from multiple_encounters_by_patient
	group by person_id

)

, latest_patient_encounters as (
	
	select
		  max_encounter_dates_by_patient.person_id
		, max_encounter_dates_by_patient.max_encounter_date
		, procedure_encounter_date
		, claims_encounter_date
	from max_encounter_dates_by_patient
	inner join multiple_encounters_by_patient
		on max_encounter_dates_by_patient.person_id = multiple_encounters_by_patient.person_id

)

, patients_with_age as (

    select
          p.person_id
        , procedure_encounter_date
        , claims_encounter_date
        , floor({{ datediff('birth_date', 'e.max_encounter_date', 'hour') }} / 8760.0) as max_age
    from {{ ref('quality_measures__stg_core__patient') }} p
    inner join latest_patient_encounters e
        on p.person_id = e.person_id
    where p.death_date is null

)

, qualifying_patients as (

    select
        distinct
          patients_with_age.person_id
        , patients_with_age.max_age as age
        , patients_with_age.procedure_encounter_date
        , patients_with_age.claims_encounter_date
        , pp.performance_period_begin
        , pp.performance_period_end
        , pp.measure_id
        , pp.measure_name
        , pp.measure_version
        , 1 as denominator_flag
    from patients_with_age
    cross join {{ ref('quality_measures__int_cqm130__performance_period') }} pp
    where max_age >= 18
    
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(age as integer) as age
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(procedure_encounter_date as date) as procedure_encounter_date
        , cast(claims_encounter_date as date) as claims_encounter_date
        , cast(denominator_flag as integer) as denominator_flag
    from qualifying_patients

)

select 
      person_id
    , age
    , procedure_encounter_date
    , claims_encounter_date
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , denominator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
