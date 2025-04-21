{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
with unioned as (

    {{ dbt_utils.union_relations(

        relations=[
            ref('fhir_preprocessing__int_medical_claim_eob'),
            ref('fhir_preprocessing__int_pharmacy_claim_eob')
        ]

    ) }}

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
from unioned
