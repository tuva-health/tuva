{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
with unioned as (

    {{ dbt_utils.union_relations(

        relations=[
            ref('fhir_preprocessing__int_lab_result'),
            ref('fhir_preprocessing__int_observation')
        ]

    ) }}

)

select
      patient_internal_id
    , resource_internal_id
    , encounter_internal_id
    , observation_status
    , observation_category
    , observation_code_system
    , observation_code
    , observation_code_text
    , observation_datetime
    , observation_value
    , observation_value_units
from unioned
