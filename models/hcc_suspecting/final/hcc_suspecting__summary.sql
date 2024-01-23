{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False))))
   )
}}

with patients as (

    select
          patient_id
        , sex
        , birth_date
        , floor({{ datediff('birth_date', 'current_date', 'hour') }} / 8766.0) as age
    from {{ ref('hcc_suspecting__stg_core__patient') }}
    where death_date is null

)

, suspecting_list as (

      select
          patient_id
        , count(*) as gaps
    from {{ ref('hcc_suspecting__list') }}
    group by patient_id

)

, joined as (

    select
          patients.patient_id
        , patients.sex
        , patients.birth_date
        , patients.age
        , suspecting_list.gaps
    from patients
         inner join suspecting_list
         on patients.patient_id = suspecting_list.patient_id

)

, add_data_types as (

    select
          cast(patient_id as {{ dbt.type_string() }}) as patient_id
        , cast(sex as {{ dbt.type_string() }}) as patient_sex
        , cast(birth_date as date) as patient_birth_date
        , cast(age as integer) as patient_age
        , cast(gaps as integer) as suspecting_gaps
    from joined

)

select
      patient_id
    , patient_sex
    , patient_birth_date
    , patient_age
    , suspecting_gaps
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from add_data_types