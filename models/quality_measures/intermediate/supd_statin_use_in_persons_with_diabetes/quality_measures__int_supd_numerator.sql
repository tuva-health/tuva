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
    from {{ ref('quality_measures__int_supd_denominator') }}

)

, statin_codes as (

    select
        code
      , code_system
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
        'pqa statin medications'
    )

)

, patients_taking_statin_meds as (

    select
        person_id
      , dispensing_date
    from {{ ref('quality_measures__stg_pharmacy_claim') }} as pharmacy_claims
    inner join statin_codes
        on pharmacy_claims.ndc_code = statin_codes.code
            and statin_codes.code_system = 'ndc'

)

, qualifying_patients_in_deno as (

    select
          patients_taking_statin_meds.person_id
        , patients_taking_statin_meds.dispensing_date as evidence_date
    from patients_taking_statin_meds
    inner join denominator
      on patients_taking_statin_meds.person_id = denominator.person_id
        and dispensing_date between
          denominator.performance_period_begin and denominator.performance_period_end

)

, numerator as (

    select
          person_id
        , evidence_date
        , 1 as numerator_flag
    from qualifying_patients_in_deno

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(evidence_date as date) as evidence_date
        , cast(null as {{ dbt.type_string() }}) as evidence_value
        , cast(numerator_flag as integer) as numerator_flag
    from numerator

)

select
      person_id
    , evidence_date
    , evidence_value
    , numerator_flag
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
