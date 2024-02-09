{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}
/*
Steps for staging the medical claim data:
    1) Filter to risk-adjustable claims per claim type for the collection year.
    2) Gather diagnosis codes from Condition for the eligible claims.
    3) Map and filter diagnosis codes to HCCs for each CMS model version
    4) Union results from each CMS model version
       (note: some payment years may not have results for v28)
*/

with conditions as (

    select
          patient_id
        , condition_code
        , payment_year
    from {{ ref('cms_hcc__int_eligible_conditions') }}

)

, seed_hcc_mapping as (

    select
          payment_year
        , diagnosis_code
        , cms_hcc_v24
        , cms_hcc_v24_flag
        , cms_hcc_v28
        , cms_hcc_v28_flag
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}

)

/* casting hcc_code to avoid formatting changes during union */
, v24_mapped as (

    select distinct
          conditions.patient_id
        , conditions.condition_code
        , conditions.payment_year
        , 'CMS-HCC-V24' as model_version
        , cast(seed_hcc_mapping.cms_hcc_v24 as {{ dbt.type_string() }}) as hcc_code
    from conditions
        inner join seed_hcc_mapping
            on conditions.condition_code = seed_hcc_mapping.diagnosis_code
            and conditions.payment_year = seed_hcc_mapping.payment_year
    where cms_hcc_v24_flag = 'Yes'

)

, v28_mapped as (

    select distinct
          conditions.patient_id
        , conditions.condition_code
        , conditions.payment_year
        , 'CMS-HCC-V28' as model_version
        , cast(seed_hcc_mapping.cms_hcc_v28 as {{ dbt.type_string() }}) as hcc_code
    from conditions
        inner join seed_hcc_mapping
            on conditions.condition_code = seed_hcc_mapping.diagnosis_code
            and conditions.payment_year = seed_hcc_mapping.payment_year
    where cms_hcc_v28_flag = 'Yes'

)

, unioned as (

    select * from v24_mapped
    union all
    select * from v28_mapped

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(condition_code as {{ dbt.type_string() }}) as condition_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from unioned

)

select
      patient_id
    , condition_code
    , hcc_code
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types