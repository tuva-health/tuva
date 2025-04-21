{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with eligibility as (

    select
          person_id
        , eligibility_id
        , payer
        , plan
        , enrollment_start_date
        , enrollment_end_date
    from {{ ref('fhir_preprocessing__stg_core__eligibility') }}

)

, claim_supporting_info as (

    select
          claim_id
        , eob_supporting_info_list
    from {{ ref('fhir_preprocessing__int_pharmacy_claim_supporting_info') }}

)

, claim_item as (

    select
          claim_id
        , eob_item_list
    from {{ ref('fhir_preprocessing__int_pharmacy_claim_item') }}

)

, claim_total as (

    select
          claim_id
        , eob_total_list
    from {{ ref('fhir_preprocessing__int_pharmacy_claim_total') }}

)

, pharmacy_eob as (

    select
          pharmacy_claim.person_id as patient_internal_id
        , pharmacy_claim.pharmacy_claim_id as resource_internal_id
        , pharmacy_claim.claim_id as unique_claim_id
        , 'pharmacy' as eob_type_code
        , null as eob_subtype_code /* required for union with medical eob */
        , null as eob_billable_period_start /* required for union with medical eob */
        , null as eob_billable_period_end /* required for union with medical eob */
        , pharmacy_claim.paid_date as eob_created
        , pharmacy_claim.payer as organization_name
        , pharmacy_claim.dispensing_provider_id as practitioner_internal_id
        , pharmacy_claim.dispensing_provider_name as practitioner_name_text
        , eligibility.eligibility_id as coverage_internal_id
        , null as eob_diagnosis_list /* required for union with medical eob */
        , null as eob_procedure_list /* required for union with medical eob */
        , claim_supporting_info.eob_supporting_info_list
        , claim_item.eob_item_list
        , claim_total.eob_total_list
    from {{ ref('fhir_preprocessing__stg_core__pharmacy_claim') }} as pharmacy_claim
        left outer join eligibility
            on pharmacy_claim.person_id = eligibility.person_id
            and pharmacy_claim.payer = eligibility.payer
            and pharmacy_claim.plan = eligibility.plan
            and pharmacy_claim.paid_date
                between eligibility.enrollment_start_date
                and eligibility.enrollment_end_date
        left outer join claim_supporting_info
            on pharmacy_claim.claim_id = claim_supporting_info.claim_id
        left outer join claim_item
            on pharmacy_claim.claim_id = claim_item.claim_id
        left outer join claim_total
            on pharmacy_claim.claim_id = claim_total.claim_id

)

select
      patient_internal_id
    , resource_internal_id
    , unique_claim_id
    , eob_type_code
    , eob_subtype_code
    , eob_billable_period_start
    , eob_billable_period_end
    , eob_created
    , organization_name
    , practitioner_internal_id
    , practitioner_name_text
    , coverage_internal_id
    , eob_diagnosis_list
    , eob_procedure_list
    , eob_supporting_info_list
    , eob_item_list
    , eob_total_list
from pharmacy_eob
