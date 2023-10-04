{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with all_conditions as (

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
    from {{ ref('cms_hcc__int_all_conditions') }}

)

, grouped as (

    select
          patient_id
        , cms_hcc_v24
        , cms_hcc_v24_description
        , model_version
        , payment_year
        , min(recorded_date) as first_recorded
        , max(recorded_date) as last_recorded
    from all_conditions
    where cms_hcc_v24 is not null
    group by
          patient_id
        , cms_hcc_v24
        , cms_hcc_v24_description
        , model_version
        , payment_year

)

, add_payment_year_flag as (

    select
          patient_id
        , cms_hcc_v24
        , cms_hcc_v24_description
        , model_version
        , payment_year
        , first_recorded
        , last_recorded
        , case
            when extract(year from last_recorded) = payment_year
            then 1
            else 0
          end as payment_year_recorded
    from grouped

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(cms_hcc_v24 as {{ dbt.type_string() }}) as cms_hcc_v24
        , cast(cms_hcc_v24_description as {{ dbt.type_string() }}) as cms_hcc_v24_description
        , cast(first_recorded as date) as first_recorded
        , cast(last_recorded as date) as last_recorded
        , cast(payment_year_recorded as boolean) as payment_year_recorded
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from add_payment_year_flag

)

select
      patient_id
    , cms_hcc_v24
    , cms_hcc_v24_description
    , first_recorded
    , last_recorded
    , payment_year_recorded
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types