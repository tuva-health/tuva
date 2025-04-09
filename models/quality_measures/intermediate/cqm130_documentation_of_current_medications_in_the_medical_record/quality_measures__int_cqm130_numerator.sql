{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with denominator as (

    select
          person_id
        , procedure_encounter_date
        , claims_encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
    from {{ ref('quality_measures__int_cqm130_denominator') }}

)

, medication_code as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'eligible clinician attests to documenting current medications'
    )

)

, procedures as (

    select
        person_id
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

, documenting_meds_procedures as (

    select
          person_id
        , procedure_date
    from procedures
    inner join medication_code
      on procedures.code = medication_code.code
        and procedures.code_type = medication_code.code_system

)

, documenting_meds_claims as (

    select
          person_id
        , coalesce(claim_end_date, claim_start_date) as encounter_date
    from {{ ref('quality_measures__stg_medical_claim') }} as medical_claim
    inner join medication_code
        on medical_claim.hcpcs_code = medication_code.code
          and medication_code.code_system = 'hcpcs'

)

, qualifying_procedure as (

    select
          documenting_meds_procedures.person_id
        , documenting_meds_procedures.procedure_date as encounter_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
    from documenting_meds_procedures
    inner join denominator
      on documenting_meds_procedures.person_id = denominator.person_id
        and documenting_meds_procedures.procedure_date = denominator.procedure_encounter_date

)

, qualifying_claims as (

    select
          documenting_meds_claims.person_id
        , documenting_meds_claims.encounter_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
    from documenting_meds_claims
    inner join denominator
      on documenting_meds_claims.person_id = denominator.person_id
        and documenting_meds_claims.encounter_date = denominator.claims_encounter_date

)

, qualifying_cares as (

    select
          person_id
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , cast(1 as integer) as numerator_flag
    from qualifying_procedure

    union all

    select
          person_id
        , encounter_date
        , performance_period_begin
        , performance_period_end
        , measure_id
        , measure_name
        , measure_version
        , cast(1 as integer) as numerator_flag
    from qualifying_claims

)

, add_data_types as (

     select distinct
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(performance_period_begin as date) as performance_period_begin
        , cast(performance_period_end as date) as performance_period_end
        , cast(measure_id as {{ dbt.type_string() }}) as measure_id
        , cast(measure_name as {{ dbt.type_string() }}) as measure_name
        , cast(measure_version as {{ dbt.type_string() }}) as measure_version
        , cast(encounter_date as date) as evidence_date
        , cast(null as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
      from qualifying_cares

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
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
