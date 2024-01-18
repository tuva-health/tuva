{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
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
    from {{ ref('hcc_suspecting__int_all_conditions') }}
    where hcc_code is not null

)

, grouped as (

    select
          patient_id
        , hcc_code
        , hcc_description
        , min(recorded_date) as first_recorded
        , max(recorded_date) as last_recorded
    from all_conditions
    where hcc_code is not null
    group by
          patient_id
        , hcc_code
        , hcc_description

)

, add_flag as (

    select
          patient_id
        , hcc_code
        , hcc_description
        , first_recorded
        , last_recorded
        , case
            when extract(year from last_recorded) = extract(year from {{ dbt.current_timestamp() }} )
            then 1
            else 0
          end as current_year_recorded
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
        , add_flag.first_recorded
        , add_flag.last_recorded
        , add_flag.current_year_recorded
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
        , cast(current_year_recorded as boolean) as current_year_recorded
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
    , current_year_recorded
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types