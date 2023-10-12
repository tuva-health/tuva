{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
Steps for staging the medical claim data:
    1) Filter to risk-adjustable claims per claim type for the collection year.
    2) Gather diagnosis codes from Condition for the eligible claims.
    3) Map and filter diagnosis codes to HCCs

Jinja is used to set payment and collection year variables.
 - The hcc_model_version and payment_year vars have been set here
   so they get compiled.
 - The collection year is one year prior to the payment year.
*/

{% set model_version_compiled = var('cms_hcc_model_version') -%}
{% set payment_year_compiled = var('cms_hcc_payment_year') -%}

with conditions as (

    select
          patient_id
        , condition_code
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_eligible_conditions') }}

)

/*
    Using jinja to choose the correct column based on hcc_model_version var.
*/
, seed_hcc_mapping as (

    select
          diagnosis_code
        , cms_hcc_v24 as hcc_code /* will be replaced with logic to use correct col based on version var */
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}
    where payment_year = {{ payment_year_compiled }}
    and cms_hcc_v24_flag = 'Yes' /* will be replaced with logic to use correct col based on version var */

)

, mapped as (

    select distinct
          conditions.patient_id
        , conditions.condition_code
        , conditions.model_version
        , conditions.payment_year
        , seed_hcc_mapping.hcc_code
    from conditions
         inner join seed_hcc_mapping
         on conditions.condition_code = seed_hcc_mapping.diagnosis_code

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(condition_code as {{ dbt.type_string() }}) as condition_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from mapped

)

select
      patient_id
    , condition_code
    , hcc_code
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types