{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}
/*
    Staging HCCs that will be used in disease factor calculations.
    Final output for each CMS model version includes:
        - hccs without a hierarchy
        - lower-level hccs with hierarchy where top-level hcc is missing
        - top-level hccs from hierarchy
*/

with hcc_mapping as (

    select distinct
          person_id
        , hcc_code
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
    from {{ ref('cms_hcc__int_hcc_mapping') }}

)

, seed_hcc_hierarchy as (

    select
          model_version
        , hcc_code
        , description
        , hccs_to_exclude
    from {{ ref('cms_hcc__disease_hierarchy') }}

)

/*
    selecting hccs that do not have a hierarchy
    all codes in this cte are included in final output
*/
, hccs_without_hierarchy as (

    select distinct
          hcc_mapping.person_id
        , hcc_mapping.model_version
        , hcc_mapping.payment_year
        , hcc_mapping.collection_start_date
        , hcc_mapping.collection_end_date
        , hcc_mapping.hcc_code
    from hcc_mapping
        left outer join seed_hcc_hierarchy as hcc_top_level
            on hcc_mapping.hcc_code = hcc_top_level.hcc_code
            and hcc_mapping.model_version = hcc_top_level.model_version
        left outer join seed_hcc_hierarchy as hcc_exclusions
            on hcc_mapping.hcc_code = hcc_exclusions.hccs_to_exclude
            and hcc_mapping.model_version = hcc_exclusions.model_version
    where hcc_top_level.hcc_code is null
        and hcc_exclusions.hccs_to_exclude is null

)

/*
    selecting hccs that have a hierarchy to be evaluated in the next cte
*/
, hccs_with_hierarchy as (

    select
          hcc_mapping.person_id
        , hcc_mapping.model_version
        , hcc_mapping.payment_year
        , hcc_mapping.collection_start_date
        , hcc_mapping.collection_end_date
        , hcc_mapping.hcc_code
        , seed_hcc_hierarchy.hcc_code as top_level_hcc
    from hcc_mapping
        inner join seed_hcc_hierarchy
            on hcc_mapping.hcc_code = seed_hcc_hierarchy.hccs_to_exclude
            and hcc_mapping.model_version = seed_hcc_hierarchy.model_version

)

/*
    applying hcc hierarchy and grouping by patient and hcc
    to account for multiple hcc combinations
    minimum HCC is included following CMS's severity logic
*/
, hierarchy_applied as (

    select
          hccs_with_hierarchy.person_id
        , hccs_with_hierarchy.model_version
        , hccs_with_hierarchy.payment_year
        , hccs_with_hierarchy.collection_start_date
        , hccs_with_hierarchy.collection_end_date
        , hccs_with_hierarchy.hcc_code
        , min(hcc_mapping.hcc_code) as top_level_hcc
    from hccs_with_hierarchy
        left outer join hcc_mapping
            on hcc_mapping.person_id = hccs_with_hierarchy.person_id
            and hcc_mapping.hcc_code = hccs_with_hierarchy.top_level_hcc
            and hcc_mapping.model_version = hccs_with_hierarchy.model_version
            and hcc_mapping.payment_year = hccs_with_hierarchy.payment_year
            and hcc_mapping.collection_end_date = hccs_with_hierarchy.collection_end_date
    group by
          hccs_with_hierarchy.person_id
        , hccs_with_hierarchy.model_version
        , hccs_with_hierarchy.payment_year
        , hccs_with_hierarchy.collection_start_date
        , hccs_with_hierarchy.collection_end_date
        , hccs_with_hierarchy.hcc_code

)

/*
    selecting lower-level hccs in hierarchy
    all codes in this cte are included in final output
*/
, lower_level_inclusions as (

    select distinct
          person_id
        , model_version
        , payment_year
        , collection_start_date
        , collection_end_date
        , case
            when top_level_hcc is not null then top_level_hcc
            else hcc_code
          end as hcc_code
    from hierarchy_applied

)

/*
    selecting top-level hccs not in previous steps
    all codes in this cte are included in final output
*/
, top_level_inclusions as (

    select distinct
          hcc_mapping.person_id
        , hcc_mapping.model_version
        , hcc_mapping.payment_year
        , hcc_mapping.collection_start_date
        , hcc_mapping.collection_end_date
        , hcc_mapping.hcc_code
    from hcc_mapping
        inner join seed_hcc_hierarchy
            on hcc_mapping.hcc_code = seed_hcc_hierarchy.hcc_code
            and hcc_mapping.model_version = seed_hcc_hierarchy.model_version
        left outer join lower_level_inclusions
            on hcc_mapping.person_id = lower_level_inclusions.person_id
            and hcc_mapping.hcc_code = lower_level_inclusions.hcc_code
            and hcc_mapping.model_version = lower_level_inclusions.model_version
            and hcc_mapping.payment_year = lower_level_inclusions.payment_year
            and hcc_mapping.collection_end_date = lower_level_inclusions.collection_end_date
        left outer join hierarchy_applied
            on hcc_mapping.person_id = hierarchy_applied.person_id
            and hcc_mapping.hcc_code = hierarchy_applied.hcc_code
            and hcc_mapping.model_version = hierarchy_applied.model_version
            and hcc_mapping.payment_year = hierarchy_applied.payment_year
            and hcc_mapping.collection_end_date = hierarchy_applied.collection_end_date
    where lower_level_inclusions.hcc_code is null
        and hierarchy_applied.top_level_hcc is null

)

, unioned as (

    select * from hccs_without_hierarchy
    union all
    select * from lower_level_inclusions
    union all
    select * from top_level_inclusions

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
        , cast(collection_start_date as date) as collection_start_date
        , cast(collection_end_date as date) as collection_end_date
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
    from unioned

)

select
      person_id
    , model_version
    , payment_year
    , collection_start_date
    , collection_end_date
    , hcc_code
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
