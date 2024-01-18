{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with hcc_history_suspects as (

    select distinct
          patient_id
        , hcc_code
        , hcc_description
        , 'Previously recorded HCC not documented in current year' as reason
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}
    where current_year_recorded = false

)

, unioned as (

    select * from hcc_history_suspects

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(reason as {{ dbt.type_string() }}) as reason
    from unioned

)

select
      patient_id
    , hcc_code
    , hcc_description
    , reason
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types