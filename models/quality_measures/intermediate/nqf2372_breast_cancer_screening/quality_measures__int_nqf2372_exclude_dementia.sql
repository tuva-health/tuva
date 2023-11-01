{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

/*
    Patients greater than or equal to 66 with at least one claim/encounter for frailty
    during the measurement period AND a dispensed medication for dementia during the measurement period
    or year prior to measurement period
*/

with patients_with_frailty as (

    select
          patient_id
        , performance_period_begin
        , performance_period_end
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_nqf2372__frailty') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where concept_name in (
        'Dementia Medications'
    )

)

, medications as (

    select
          patient_id
        , dispensing_date
        , source_code_type
        , source_code
        , ndc_code
        , rxnorm_code
    from {{ ref('quality_measures__stg_core__medication') }}

)

, pharmacy_claim as (

    select
          patient_id
        , dispensing_date
        , ndc_code
        , paid_date
    from {{ ref('quality_measures__stg_pharmacy_claim') }}

)

, medication_exclusions as (

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.ndc_code = exclusion_codes.code
    where exclusion_codes.code_system = 'ndc'

    union all

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.rxnorm_code = exclusion_codes.code
    where exclusion_codes.code_system = 'rxnorm'

    union all

    select
          medications.patient_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.source_code = exclusion_codes.code
            and medications.source_code_type = exclusion_codes.code_system

)

, pharmacy_claim_exclusions as (

    select
          pharmacy_claim.patient_id
        , pharmacy_claim.dispensing_date
        , pharmacy_claim.ndc_code
        , pharmacy_claim.paid_date
        , exclusion_codes.concept_name
    from pharmacy_claim
         inner join exclusion_codes
            on pharmacy_claim.ndc_code = exclusion_codes.code
    where exclusion_codes.code_system = 'ndc'

)

, frailty_with_dementia as (

    select
          patients_with_frailty.patient_id
        , patients_with_frailty.exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || pharmacy_claim_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join pharmacy_claim_exclusions
            on patients_with_frailty.patient_id = pharmacy_claim_exclusions.patient_id
    where (
        pharmacy_claim_exclusions.dispensing_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
        or pharmacy_claim_exclusions.paid_date
            between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
            and patients_with_frailty.performance_period_end
    )

    union all

    select
          patients_with_frailty.patient_id
        , medication_exclusions.dispensing_date as exclusion_date
        , patients_with_frailty.exclusion_reason
            || ' with '
            || medication_exclusions.concept_name
          as exclusion_reason
    from patients_with_frailty
         inner join medication_exclusions
         on patients_with_frailty.patient_id = medication_exclusions.patient_id
    where medication_exclusions.dispensing_date
        between {{ dbt.dateadd(datepart="year", interval=-1, from_date_or_timestamp="patients_with_frailty.performance_period_begin") }}
        and patients_with_frailty.performance_period_end

)

select
      patient_id
    , exclusion_date
    , exclusion_reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from frailty_with_dementia