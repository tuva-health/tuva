{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with hcc_history_suspects as (

    select distinct
          patient_id
        , data_source
        , hcc_code
        , hcc_description
        , 'Prior coding history' as reason
        , icd_10_cm_code
            || case
                when last_billed is not null then ' last billed on ' || last_billed
                when last_billed is null and last_recorded is not null then ' last recorded on ' || last_recorded
                else ' (missing recorded and billing dates) '
          end as contributing_factor
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}
    where current_year_billed = false

)

, unioned as (

    select * from hcc_history_suspects

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
    from unioned

)

select
      patient_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types