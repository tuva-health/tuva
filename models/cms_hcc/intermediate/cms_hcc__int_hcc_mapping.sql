{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
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
          person_id
        , payer
        , condition_code
        , payment_year
        , collection_start_date
        , collection_end_date
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

    union all

    -- Adding a mapping for the next year copying the current year mappings
    select
          payment_year + 1 as payment_year
        , diagnosis_code
        , cms_hcc_v24
        , cms_hcc_v24_flag
        , cms_hcc_v28
        , cms_hcc_v28_flag
    from {{ ref('cms_hcc__icd_10_cm_mappings') }}
    where payment_year = (select max(payment_year) as payment_year from {{ ref('cms_hcc__icd_10_cm_mappings') }})


)

/* casting hcc_code to avoid formatting changes during union */
, v24_mapped as (

    select distinct
          conditions.person_id
        , conditions.payer
        , conditions.condition_code
        , conditions.payment_year
        , conditions.collection_start_date
        , conditions.collection_end_date
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
          conditions.person_id
        , conditions.payer
        , conditions.condition_code
        , conditions.payment_year
        , conditions.collection_start_date
        , conditions.collection_end_date
        , 'CMS-HCC-V28' as model_version
        , cast(seed_hcc_mapping.cms_hcc_v28 as {{ dbt.type_string() }}) as hcc_code
    from conditions
        inner join seed_hcc_mapping
            on conditions.condition_code = seed_hcc_mapping.diagnosis_code
            and conditions.payment_year = seed_hcc_mapping.payment_year
    where cms_hcc_v28_flag = 'Yes'

)

/*
    V28 Heart Interaction Patch (CMS SAS V2825T1M.TXT lines 448-450):
    CC223 (Heart Failure, without CC specification) must be zeroed when
    no sibling heart CCs (CC221, CC222, CC224, CC225, CC226) are present.
    This runs before hierarchy suppression. V28 only — not applicable to V24.
*/
, v28_heart_sibling as (

    select distinct
          person_id
        , payer
        , payment_year
        , collection_end_date
    from v28_mapped
    where hcc_code in ('221', '222', '224', '225', '226')

)

, v28_heart_patch as (

    select
          v28_mapped.person_id
        , v28_mapped.payer
        , v28_mapped.condition_code
        , v28_mapped.payment_year
        , v28_mapped.collection_start_date
        , v28_mapped.collection_end_date
        , v28_mapped.model_version
        , v28_mapped.hcc_code
    from v28_mapped
        left join v28_heart_sibling
            on v28_mapped.person_id = v28_heart_sibling.person_id
            and v28_mapped.payer = v28_heart_sibling.payer
            and v28_mapped.payment_year = v28_heart_sibling.payment_year
            and v28_mapped.collection_end_date = v28_heart_sibling.collection_end_date
    where not (
        v28_mapped.hcc_code = '223'
        and v28_heart_sibling.person_id is null
    )

)

, unioned as (

    select * from v24_mapped
    union all
    select * from v28_heart_patch

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(payer as {{ dbt.type_string() }}) as payer
        , cast(condition_code as {{ dbt.type_string() }}) as condition_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
    from unioned

)

select
      person_id
    , payer
    , condition_code
    , hcc_code
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from add_data_types
