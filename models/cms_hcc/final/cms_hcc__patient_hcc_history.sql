{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with all_conditions as (

    select
          patient_id
        , recorded_date
        , condition_type
        , icd_10_cm_code
        , hcc_code
        , hcc_description
        , model_version
        , payment_year
    from {{ ref('cms_hcc__int_all_conditions') }}

)

, grouped as (

    select
          patient_id
        , hcc_code
        , hcc_description
        , payment_year
        , min(recorded_date) as first_recorded
        , max(recorded_date) as last_recorded
    from all_conditions
    where hcc_code is not null
    group by
          patient_id
        , hcc_code
        , hcc_description
        , payment_year

)

, add_flag as (

    select
          patient_id
        , hcc_code
        , hcc_description
        , first_recorded
        , last_recorded
        , payment_year
        , case
            when extract(year from last_recorded) = payment_year
            then 1
            else 0
          end as payment_year_recorded
    from grouped

)

, all_conditions_with_flag as (

    select
          all_conditions.patient_id
        , all_conditions.recorded_date
        , all_conditions.condition_type
        , all_conditions.icd_10_cm_code
        , all_conditions.hcc_code
        , all_conditions.hcc_description
        , all_conditions.model_version
        , all_conditions.payment_year
        , add_flag.first_recorded
        , add_flag.last_recorded
        , add_flag.payment_year_recorded
    from all_conditions
         left join add_flag
            on all_conditions.patient_id = add_flag.patient_id
            and all_conditions.hcc_code = add_flag.hcc_code

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(recorded_date as date) as recorded_date
        , cast(condition_type as {{ dbt.type_string() }}) as condition_type
        , cast(icd_10_cm_code as {{ dbt.type_string() }}) as icd_10_cm_code
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(first_recorded as date) as first_recorded
        , cast(last_recorded as date) as last_recorded
        , cast(payment_year_recorded as boolean) as payment_year_recorded
        , cast(model_version as {{ dbt.type_string() }}) as model_version
        , cast(payment_year as integer) as payment_year
    from all_conditions_with_flag

)

select
      patient_id
    , recorded_date
    , condition_type
    , icd_10_cm_code
    , hcc_code
    , hcc_description
    , first_recorded
    , last_recorded
    , payment_year_recorded
    , model_version
    , payment_year
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types