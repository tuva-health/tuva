{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
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
    from {{ ref('quality_measures__int_cqm438_denominator') }}

)

, statin_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
          'high intensity statin therapy'
        , 'low intensity statin therapy'
        , 'moderate intensity statin therapy'
        , 'statin therapy'
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

, procedure_statin_related as (

    select
          procedures.person_id
        , procedures.procedure_date as evidence_date
    from procedures
    inner join statin_codes
        on procedures.code = statin_codes.code
            and procedures.code_type = statin_codes.code_system

)

, pharmacy_claims_statin_related as (

    select
        person_id
      , dispensing_date as evidence_date
      , ndc_code
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claims
    inner join statin_codes
    on pharmacy_claims.ndc_code = statin_codes.code
        and lower(statin_codes.code_system) = 'ndc'

)

, medication_statin_related as (

    select
          person_id
        , coalesce(dispensing_date, prescribing_date) as evidence_date
        , source_code
        , source_code_type
    from {{ ref('quality_measures__stg_core__medication') }} as medications
    inner join statin_codes
        on medications.source_code = statin_codes.code
        and medications.source_code_type = statin_codes.code_system

)

, qualifying_patients as (

    select
          procedure_statin_related.person_id
        , procedure_statin_related.evidence_date
    from procedure_statin_related

    union all

    select
          pharmacy_claims_statin_related.person_id
        , pharmacy_claims_statin_related.evidence_date
    from pharmacy_claims_statin_related

    union all

    select
          medication_statin_related.person_id
        , medication_statin_related.evidence_date
    from medication_statin_related

)

, qualifying_patients_with_denominator as (

    select
          qualifying_patients.person_id
        , qualifying_patients.evidence_date
        , denominator.performance_period_begin
        , denominator.performance_period_end
        , denominator.measure_id
        , denominator.measure_name
        , denominator.measure_version
        , cast(1 as integer) as numerator_flag
    from qualifying_patients
    inner join denominator
    on qualifying_patients.person_id = denominator.person_id
    and evidence_date between
            denominator.performance_period_begin and
                denominator.performance_period_end

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
    from qualifying_patients_with_denominator

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
