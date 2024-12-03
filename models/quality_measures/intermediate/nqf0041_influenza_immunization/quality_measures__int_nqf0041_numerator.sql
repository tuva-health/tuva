{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
    | as_bool
   )
}}

with denominator as (

    select 
          person_id
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_nqf0041_denominator') }}

)

, influenza_vaccination_code as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'influenza vaccination'
        , 'influenza vaccine'
        , 'influenza virus laiv immunization'
        , 'influenza virus laiv procedure'
    )

)

, procedure_vaccination as (

    select
        person_id
      , procedure_date
    from {{ ref('quality_measures__stg_core__procedure') }} as procedures
    inner join influenza_vaccination_code
        on coalesce(procedures.normalized_code, procedures.source_code) = influenza_vaccination_code.code
            and coalesce(procedures.normalized_code_type, procedures.source_code_type) = influenza_vaccination_code.code_system

)

, claims_vaccination as (
    
    select 
          person_id
        , coalesce(claim_start_date,claim_end_date) as min_date
        , coalesce(claim_end_date,claim_start_date) as max_date
    from {{ ref('quality_measures__stg_medical_claim') }} medical_claim
    inner join influenza_vaccination_code
        on medical_claim.hcpcs_code = influenza_vaccination_code.code

)

, qualifying_procedures as (

    select
          procedure_vaccination.person_id
        , procedure_vaccination.procedure_date as evidence_date
    from procedure_vaccination
    inner join {{ ref('quality_measures__int_nqf0041__performance_period') }} pp
        on procedure_date between 
            pp.lookback_period_august and
                pp.performance_period_end

)

, qualifying_claims as (

    select 
          claims_vaccination.person_id
        , claims_vaccination.max_date as evidence_date
    from claims_vaccination
    inner join {{ ref('quality_measures__int_nqf0041__performance_period') }} pp
        on max_date between
            pp.lookback_period_august and
                pp.performance_period_end

)

, qualified_patients as (

    select
          person_id
        , evidence_date
    from qualifying_procedures

    union all

    select
          person_id
        , evidence_date
    from qualifying_claims

)

, combined_qualifying_patients as (

    select
          qualified_patients.person_id
        , qualified_patients.evidence_date
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , 1 as numerator_flag
    from qualified_patients
    inner join denominator
        on qualified_patients.person_id = denominator.person_id

)

, add_data_types as (

     select distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(evidence_date as date) as evidence_date
        , cast(null as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from combined_qualifying_patients

)

select
      person_id
    , performance_period_begin
    , performance_period_end
    , measure_id
    , measure_name
    , measure_version
    , evidence_date
    , evidence_value
    , numerator_flag
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types
