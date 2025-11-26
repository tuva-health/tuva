{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with conditions as (

    select
          person_id
        , payer
        , claim_id
        , recorded_date
        , condition_type
        , code_type
        , code
        , data_source
    from {{ ref('hcc_suspecting__int_prep_conditions') }}

)

, seed_hcc_mapping as (

    select
          payment_year
        , diagnosis_code
        , cms_hcc_v28 as hcc_code
        , 'CMS-HCC-V28' as model_version
    from {{ ref('hcc_suspecting__icd_10_cm_mappings') }}
    where cms_hcc_v28_flag = 'Yes'

    union all

    select
          payment_year
        , diagnosis_code
        , cms_hcc_v24 as hcc_code
        , 'CMS-HCC-V24' as model_version
    from {{ ref('hcc_suspecting__icd_10_cm_mappings') }}
    where cms_hcc_v24_flag = 'Yes'
)

-- Add in support for v24
, seed_hcc_descriptions as (

    select distinct
          hcc_code
        , hcc_description
        , 'CMS-HCC-V28' as model_version
    from {{ ref('hcc_suspecting__hcc_descriptions') }}

)

, joined as (

    select
          conditions.person_id
        , conditions.payer
        , conditions.claim_id
        , conditions.recorded_date
        , conditions.condition_type
        , conditions.code
        , conditions.data_source
        , seed_hcc_mapping.model_version
        , seed_hcc_mapping.hcc_code
        , seed_hcc_descriptions.hcc_description
    from conditions
         left outer join seed_hcc_mapping
            on conditions.code = seed_hcc_mapping.diagnosis_code
            and {{ date_part('year', 'conditions.recorded_date') }} + 1 = seed_hcc_mapping.payment_year
         left outer join seed_hcc_descriptions
         on seed_hcc_mapping.hcc_code = seed_hcc_descriptions.hcc_code
    where conditions.code_type = 'icd-10-cm'
)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(claim_id as {{ dbt.type_string() }}) as claim_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(code as {{ dbt.type_string() }}) as icd_10_cm_code
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from joined

)

select
      person_id
    , payer
    , claim_id
    , recorded_date
    , condition_type
    , icd_10_cm_code
    , model_version
    , hcc_code
    , hcc_description
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
