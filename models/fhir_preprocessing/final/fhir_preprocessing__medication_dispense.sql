{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      person_id as patient_internal_id
    , medication_id as resource_internal_id
    , 'completed' as medication_dispense_status
    , case
        when ndc_code is not null then 'NDC'
        when rxnorm_code is not null then 'RXNORM'
        else source_code_type
      end as medication_code_system
    , coalesce(ndc_code,rxnorm_code) as medication_code
    , coalesce(ndc_description,rxnorm_description) as medication_code_display
    , days_supply as medication_dispense_days_supply_value
    , dispensing_date as medication_dispense_when_handed_over
from {{ ref('fhir_preprocessing__stg_core__medication') }}
