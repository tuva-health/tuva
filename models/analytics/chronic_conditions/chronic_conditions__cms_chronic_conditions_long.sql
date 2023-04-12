{{ config(enabled = var('cms_chronic_conditions_enabled',var('tuva_packages_enabled',True)) ) }}

with conditions_unioned as (

    select *
    from {{ ref('chronic_conditions__stg_cms_chronic_condition_all') }}

    union distinct

    select *
    from {{ ref('chronic_conditions__stg_cms_chronic_condition_hiv_aids') }}

    union distinct

    select *
    from {{ ref('chronic_conditions__stg_cms_chronic_condition_oud') }}

)

select
      patient_id
    , encounter_id
    , encounter_start_date
    , chronic_condition_type
    , condition_category
    , condition
    , data_source
from conditions_unioned