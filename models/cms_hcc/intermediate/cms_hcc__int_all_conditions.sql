{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

{% set model_version_compiled = var('cms_hcc_model_version') -%}
{% set payment_year_compiled = var('cms_hcc_payment_year') -%}

with conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
    from {{ ref('cms_hcc__stg_core__condition') }}

)

, seed_hcc_mapping as (

    select
          diagnosis_code
        , cms_hcc_v24
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}
    where payment_year = {{ payment_year_compiled }}
    and cms_hcc_v24_flag = 'Yes'

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , description
    from {{ ref('cms_hcc__disease_factors') }}
    where model_version = '{{ model_version_compiled }}'

)

, joined as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code_type
        , conditions.code
        , seed_hcc_mapping.cms_hcc_v24
        , seed_hcc_descriptions.description as cms_hcc_v24_description
    from conditions
         left join seed_hcc_mapping
         on conditions.code = seed_hcc_mapping.diagnosis_code
         left join seed_hcc_descriptions
         on seed_hcc_mapping.cms_hcc_v24 = seed_hcc_descriptions.hcc_code
    where conditions.code_type = 'icd-10-cm'

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(code_type as {{ dbt.type_string() }}) as code_type
        , cast(code as {{ dbt.type_string() }}) as code
        , cast(cms_hcc_v24 as {{ dbt.type_string() }}) as cms_hcc_v24
        , cast(cms_hcc_v24_description as {{ dbt.type_string() }}) as cms_hcc_v24_description
        , cast('{{ model_version_compiled }}' as {{ dbt.type_string() }}) as model_version
        , cast({{ payment_year_compiled }} as integer) as payment_year
    from joined

)

select
      patient_id
    , recorded_date
    , condition_type
    , code_type
    , code
    , cms_hcc_v24
    , cms_hcc_v24_description
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types