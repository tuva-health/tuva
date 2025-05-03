{{ config(
     enabled = var('quality_measures_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',false))))
 | as_bool
   )
}}

with patients_with_frailty as (

    select
          person_id
        , exclusion_date
        , exclusion_reason
    from {{ ref('quality_measures__int_shared_exclusions_frailty') }}

)

, exclusion_codes as (

    select
          code
        , code_system
        , concept_name
    from {{ ref('quality_measures__value_sets') }}
    where lower(concept_name) in (
        'dementia medications'
    )

)

, medications as (

    select
          person_id
        , dispensing_date
        , source_code_type
        , source_code
        , ndc_code
        , rxnorm_code
    from {{ ref('quality_measures__stg_core__medication') }}

)

, pharmacy_claim as (

    select
          person_id
        , dispensing_date
        , ndc_code
        , paid_date
    from {{ ref('quality_measures__stg_pharmacy_claim') }}

)

, medication_exclusions as (

    select
          medications.person_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.ndc_code = exclusion_codes.code
    where exclusion_codes.code_system = 'ndc'

    union all

    select
          medications.person_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.rxnorm_code = exclusion_codes.code
    where exclusion_codes.code_system = 'rxnorm'

    union all

    select
          medications.person_id
        , medications.dispensing_date
        , exclusion_codes.concept_name
    from medications
         inner join exclusion_codes
            on medications.source_code = exclusion_codes.code
            and medications.source_code_type = exclusion_codes.code_system

)

, pharmacy_claim_exclusions as (

    select
          pharmacy_claim.person_id
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
          patients_with_frailty.person_id
        , patients_with_frailty.exclusion_date
        , {{ concat_custom([
            "patients_with_frailty.exclusion_reason",
            "' with '",
            "pharmacy_claim_exclusions.concept_name"
        ]) }} as exclusion_reason
        , pharmacy_claim_exclusions.dispensing_date
        , pharmacy_claim_exclusions.paid_date
    from patients_with_frailty
         inner join pharmacy_claim_exclusions
            on patients_with_frailty.person_id = pharmacy_claim_exclusions.person_id

    union all

    select
          patients_with_frailty.person_id
        , medication_exclusions.dispensing_date as exclusion_date
        , {{ concat_custom([
            "patients_with_frailty.exclusion_reason",
            "' with '",
            "medication_exclusions.concept_name"
        ]) }} as exclusion_reason
        , medication_exclusions.dispensing_date
        , null as paid_date
    from patients_with_frailty
         inner join medication_exclusions
         on patients_with_frailty.person_id = medication_exclusions.person_id

)

select
      person_id
    , exclusion_date
    , exclusion_reason
    , 'dementia' as exclusion_type
    , dispensing_date
    , paid_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from frailty_with_dementia
