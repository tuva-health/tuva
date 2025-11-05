{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with hcc_history_suspects as (

    select distinct
          person_id
        , payer
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , current_year_billed
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}


)

, comorbidity_suspects as (

    select distinct
          person_id
        , payer
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , current_year_billed
    from {{ ref('hcc_suspecting__int_comorbidity_suspects') }}
)

, observation_suspects as (

    select distinct
          person_id
        , payer
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , current_year_billed
    from {{ ref('hcc_suspecting__int_observation_suspects') }}
)

, lab_suspects as (

    select distinct
          person_id
        , payer
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , current_year_billed
    from {{ ref('hcc_suspecting__int_lab_suspects') }}
)

, medication_suspects as (

    select distinct
          person_id
        , payer
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
        , current_year_billed
    from {{ ref('hcc_suspecting__int_medication_suspects') }}
)

, unioned as (

    select * from hcc_history_suspects
    union all
    select * from comorbidity_suspects
    union all
    select * from observation_suspects
    union all
    select * from lab_suspects
    union all
    select * from medication_suspects

)

select
      cast(person_id as {{ dbt.type_string() }}) as person_id
    , cast(payer as {{ dbt.type_string() }}) as payer
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
    , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
    , cast(reason as {{ dbt.type_string() }}) as reason
    , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
    , cast(suspect_date as date) as suspect_date
    , cast(current_year_billed as boolean) as current_year_billed
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from unioned