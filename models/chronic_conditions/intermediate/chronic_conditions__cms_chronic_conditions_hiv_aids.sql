{{ config(
     enabled = var('cms_chronic_conditions_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

{%- set condition_filter = 'Human Immunodeficiency Virus and/or Acquired Immunodeficiency Syndrome (HIV/AIDS)' -%}

with chronic_conditions as (

    select * from {{ ref('chronic_conditions__cms_chronic_conditions_hierarchy') }}
    where condition = '{{ condition_filter }}'

)

, patient_conditions as (

    select
          person_id
        , claim_id
        , recorded_date as start_date
        , normalized_code_type as code_type
        , replace(normalized_code, '.', '') as code
        , data_source
    from {{ ref('cms_chronic_conditions__stg_core__condition') }}

)

, patient_ms_drgs as (

    select
          person_id
        , claim_id
        , claim_start_date as start_date
        , drg_code_type as drg_code_type
        , drg_code as code
        , data_source
    from {{ ref('cms_chronic_conditions__stg_core__medical_claim') }}
    where
        drg_code_type = 'ms-drg'

)

/*
    Exception logic: a claim with the diagnosis code R75 requires a second
    qualifying claim that is not R75 (a screening code)

    This CTE excludes encounters with the exception code. Those encounters
    will be evaluated separately.
*/
, inclusions_diagnosis as (

    select
          patient_conditions.person_id
        , patient_conditions.claim_id
        , patient_conditions.start_date
        , patient_conditions.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_conditions
         inner join chronic_conditions
             on patient_conditions.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'ICD-10-CM'
    and chronic_conditions.code <> 'R75'

)

, inclusions_ms_drg as (

    select
          patient_ms_drgs.person_id
        , patient_ms_drgs.claim_id
        , patient_ms_drgs.start_date
        , patient_ms_drgs.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_ms_drgs
         inner join chronic_conditions
             on patient_ms_drgs.code = chronic_conditions.code
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'MS-DRG'

)

/*
    Exception logic: a claim with the diagnosis code R75 requires a second
    qualifying claim that is not R75 (a screening code)

    This CTE includes encounters with the exception code only where that
    patient has another encounter that is not R75.
*/
, exception_diagnosis as (

    select
          patient_conditions.person_id
        , patient_conditions.claim_id
        , patient_conditions.start_date
        , patient_conditions.data_source
        , chronic_conditions.chronic_condition_type
        , chronic_conditions.condition_category
        , chronic_conditions.condition
    from patient_conditions
         inner join chronic_conditions
             on patient_conditions.code = chronic_conditions.code
         inner join inclusions_diagnosis
             on patient_conditions.person_id = inclusions_diagnosis.person_id
    where chronic_conditions.inclusion_type = 'Include'
    and chronic_conditions.code_system = 'ICD-10-CM'
    and chronic_conditions.code = 'R75'

)

{% if target.type == 'fabric' %}
, inclusions_unioned as (

    select * from inclusions_diagnosis
    union
    select * from inclusions_ms_drg
    union
    select * from exception_diagnosis

)
{% else %}
, inclusions_unioned as (

    select * from inclusions_diagnosis
    union distinct
    select * from inclusions_ms_drg
    union distinct
    select * from exception_diagnosis

)
{% endif %}

select distinct
      cast(inclusions_unioned.person_id as {{ dbt.type_string() }}) as person_id
    , cast(inclusions_unioned.claim_id as {{ dbt.type_string() }}) as claim_id
    , cast(inclusions_unioned.start_date as date) as start_date
    , cast(inclusions_unioned.chronic_condition_type as {{ dbt.type_string() }}) as chronic_condition_type
    , cast(inclusions_unioned.condition_category as {{ dbt.type_string() }}) as condition_category
    , cast(inclusions_unioned.condition as {{ dbt.type_string() }}) as condition
    , cast(inclusions_unioned.data_source as {{ dbt.type_string() }}) as data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from inclusions_unioned
