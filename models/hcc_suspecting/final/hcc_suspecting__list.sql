{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with hcc_history_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , cast('Prior coding history' as {{ dbt.type_string() }}) as reason
        , icd_10_cm_code
            || case
                when last_billed is not null then ' last billed on ' || last_billed
                when last_billed is null and last_recorded is not null then ' last recorded on ' || last_recorded
                else ' (missing recorded and billing dates) '
          end as contributing_factor
        , coalesce(last_billed, last_recorded) as condition_date
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}
    where (current_year_billed = false
        or current_year_billed is null)

)

, comorbidity_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , cast('Comorbidity suspect' as {{ dbt.type_string() }}) as reason
        , condition_1_concept_name
            || ' ('
            || condition_1_code
            || ' on '
            || condition_1_recorded_date
            || ')'
            || ' and '
            || condition_2_concept_name
            || ' ('
            || condition_2_code
            || ' on '
            || condition_2_recorded_date
            || ')'
          as contributing_factor
        , condition_1_recorded_date as condition_date
    from {{ ref('hcc_suspecting__int_comorbidity_suspects') }}
    where (current_year_billed = false
        or current_year_billed is null)

)

, observation_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , cast('Observation suspect' as {{ dbt.type_string() }}) as reason
        , 'BMI result '
            || cast(observation_result as {{ dbt.type_string() }})
            || case
                when condition_code is null then ''
                else ' with '
                    || condition_concept_name
                    || ' ('
                    || condition_code
                    || ' on '
                    || condition_date
                    || ')'
                end
          as contributing_factor
        , observation_date as condition_date
    from {{ ref('hcc_suspecting__int_observation_suspects') }}
    where (current_year_billed = false
        or current_year_billed is null)

)

, unioned as (

    select * from hcc_history_suspects
    union all
    select * from comorbidity_suspects
    union all
    select * from observation_suspects

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
        , cast(condition_date as date) as condition_date
    from unioned

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , condition_date
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types