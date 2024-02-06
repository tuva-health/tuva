{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__int_prep_conditions') }}

)

, seed_hcc_mapping as (

    select
          diagnosis_code
        , cms_hcc_v28 as hcc_code
    from {{ ref('hcc_suspecting__icd_10_cm_mappings') }}
    where cms_hcc_v28 is not null

)

, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
    from {{ ref('hcc_suspecting__hcc_descriptions') }}

)

, joined as (

    select
          conditions.patient_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code
        , conditions.data_source
        , seed_hcc_mapping.hcc_code
        , seed_hcc_descriptions.hcc_description
    from conditions
         left join seed_hcc_mapping
         on conditions.code = seed_hcc_mapping.diagnosis_code
         left join seed_hcc_descriptions
         on seed_hcc_mapping.hcc_code = seed_hcc_descriptions.hcc_code
    where conditions.code_type = 'icd-10-cm'

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(code as {{ dbt.type_string() }}) as icd_10_cm_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from joined

)

select
      patient_id
    , recorded_date
    , condition_type
    , icd_10_cm_code
    , hcc_code
    , hcc_description
    , data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types