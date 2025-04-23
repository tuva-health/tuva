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

, claim_diagnosis as (

    select
          claim_id
        , eob_diagnosis_list
    from {{ ref('fhir_preprocessing__int_medical_claim_diagnosis') }}

)

, claim_procedure as (

    select
          claim_id
        , eob_procedure_list
    from {{ ref('fhir_preprocessing__int_medical_claim_procedure') }}

)

, claim_supporting_info as (

    select
          claim_id
        , eob_supporting_info_list
    from {{ ref('fhir_preprocessing__int_medical_claim_supporting_info') }}

)

, claim_item as (

    select
          claim_id
        , eob_item_list
    from {{ ref('fhir_preprocessing__int_medical_claim_item') }}

)

, claim_total as (

    select
          claim_id
        , eob_total_list
    from {{ ref('fhir_preprocessing__int_medical_claim_total') }}

)

/* add window function to dedupe if there is overlapping coverage */
, add_coverage as (

    select
          medical_claim.person_id
        , medical_claim.medical_claim_id
        , medical_claim.claim_id
        , medical_claim.claim_type
        , medical_claim.encounter_group
        , medical_claim.claim_start_date
        , medical_claim.claim_end_date
        , medical_claim.paid_date
        , medical_claim.payer
        , medical_claim.billing_id
        , medical_claim.billing_name
        , eligibility.eligibility_id
        , row_number() over (
            partition by
                  medical_claim.person_id
                , medical_claim.medical_claim_id
            order by eligibility.enrollment_start_date desc
        ) as coverage_row_num
    from {{ ref('fhir_preprocessing__stg_core__medical_claim') }} as medical_claim
        left outer join eligibility
            on medical_claim.person_id = eligibility.person_id
            and medical_claim.payer = eligibility.payer
            and medical_claim.plan = eligibility.plan
            and medical_claim.claim_start_date
                between eligibility.enrollment_start_date
                and eligibility.enrollment_end_date
    where medical_claim.claim_line_number = 1 /* filter to claim header */

)

, dedupe as (

    select
          person_id
        , medical_claim_id
        , claim_id
        , claim_type
        , encounter_group
        , claim_start_date
        , claim_end_date
        , paid_date
        , payer
        , billing_id
        , billing_name
        , eligibility_id
    from add_coverage
    where coverage_row_num = 1

)

, medical_eob as (

    select
          medical_claim.person_id as patient_internal_id
        , medical_claim.medical_claim_id as resource_internal_id
        , medical_claim.claim_id as unique_claim_id
        , medical_claim.claim_type as eob_type_code
        , case
            when medical_claim.encounter_group = 'inpatient' then 'inpatient'
            else 'outpatient'
          end as eob_subtype_code
        , medical_claim.claim_start_date as eob_billable_period_start
        , medical_claim.claim_end_date as eob_billable_period_end
        , coalesce(
              medical_claim.paid_date
            , medical_claim.claim_start_date
          ) as eob_created
        , medical_claim.payer as organization_name
        , medical_claim.billing_id as practitioner_internal_id
        , medical_claim.billing_name as practitioner_name_text
        , medical_claim.eligibility_id as coverage_internal_id
        , claim_diagnosis.eob_diagnosis_list
        , claim_procedure.eob_procedure_list
        , claim_supporting_info.eob_supporting_info_list
        , claim_item.eob_item_list
        , claim_total.eob_total_list
    from dedupe as medical_claim
        left outer join claim_diagnosis
            on medical_claim.claim_id = claim_diagnosis.claim_id
        left outer join claim_procedure
            on medical_claim.claim_id = claim_procedure.claim_id
        left outer join claim_supporting_info
            on medical_claim.claim_id = claim_supporting_info.claim_id
        left outer join claim_item
            on medical_claim.claim_id = claim_item.claim_id
        left outer join claim_total
            on medical_claim.claim_id = claim_total.claim_id

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
from medical_eob
