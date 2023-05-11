{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('tuva_marts_enabled',True))
   )
}}

with conditions_unioned as (

    select *
    from {{ ref('chronic_conditions__cms_chronic_conditions_all') }}

    union distinct

    select *
    from {{ ref('chronic_conditions__cms_chronic_conditions_hiv_aids') }}

    union distinct

    select *
    from {{ ref('chronic_conditions__cms_chronic_conditions_oud') }}

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