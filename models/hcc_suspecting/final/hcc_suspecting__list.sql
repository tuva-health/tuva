{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with hcc_history_suspects as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from {{ ref('hcc_suspecting__int_patient_hcc_history') }}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}

)

, comorbidity_suspects as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from {{ ref('hcc_suspecting__int_comorbidity_suspects') }}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}

)

, observation_suspects as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from {{ ref('hcc_suspecting__int_observation_suspects') }}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}

)

, lab_suspects as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from {{ ref('hcc_suspecting__int_lab_suspects') }}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}

)

, medication_suspects as (

    select distinct
          person_id
        , data_source
        , hcc_code
        , hcc_description
        , reason
        , contributing_factor
        , suspect_date
    from {{ ref('hcc_suspecting__int_medication_suspects') }}
    {% if target.type == 'fabric' %}
        where (current_year_billed = 0
            or current_year_billed is null)
    {% else %}
        where (current_year_billed = false
            or current_year_billed is null)
    {% endif %}

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

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(hcc_code as {{ dbt.type_string() }}) as hcc_code
        , cast(hcc_description as {{ dbt.type_string() }}) as hcc_description
        , cast(reason as {{ dbt.type_string() }}) as reason
        , cast(contributing_factor as {{ dbt.type_string() }}) as contributing_factor
        , cast(suspect_date as date) as suspect_date
    from unioned

)

select
      person_id
    , data_source
    , hcc_code
    , hcc_description
    , reason
    , contributing_factor
    , suspect_date
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
