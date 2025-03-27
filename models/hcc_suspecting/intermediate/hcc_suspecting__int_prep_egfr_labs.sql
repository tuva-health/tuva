{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with lab_result as (

    select
          person_id
        , data_source
        , code_type
        , code
        , status
        , result
        , result_date
    from {{ ref('hcc_suspecting__stg_core__lab_result') }}

)

, seed_egfr_codes as (

    select
          concept_name
        , code
        , code_system
    from {{ ref('hcc_suspecting__clinical_concepts') }}
    where lower(concept_name) = 'estimated glomerular filtration rate'

)

, egfr_labs as (

    select distinct
          lab_result.person_id
        , lab_result.data_source
        , lab_result.code_type
        , lab_result.code
        , lab_result.result_date
        , lab_result.result
    from lab_result
        inner join seed_egfr_codes
        on lab_result.code = seed_egfr_codes.code
        and lab_result.code_type = seed_egfr_codes.code_system
    where lab_result.result is not null
    and lower(lab_result.status) not in ('cancelled', 'entered-in-error')

)

, numeric_egfr_labs as (

    select
          person_id
        , data_source
        , code_type
        , code
        , result_date
        , cast(result as {{ dbt.type_numeric() }}) as result
    from egfr_labs
   {% if target.type == 'fabric' %}
        WHERE result LIKE '%.%' OR result LIKE '%[0-9]%'
        AND result NOT LIKE '%[^0-9.]%'
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }}
    {% endif %}

)

, clean_non_numeric_egfr_labs as (

    select
          person_id
        , data_source
        , code_type
        , code
        , result_date
        , result
        , cast(case
            when lower(result) like '%unsatisfactory specimen%' then null
            when result like '%>%' then null
            when result like '%<%' then null
            when result like '%@%' then trim(replace(result, '@', ''))
            when result like '%mL/min/1.73m2%' then trim(replace(result, 'mL/min/1.73m2', ''))
            else null
          end as {{ dbt.type_numeric() }}) as clean_result
    from egfr_labs
    {% if target.type == 'fabric' %}
        WHERE NOT (result LIKE '%.%' OR result LIKE '%[0-9]%'
        AND result NOT LIKE '%[^0-9.]%')
    {% else %}
        where {{ apply_regex('result', '^[+-]?([0-9]*[.])?[0-9]+$') }} = false
    {% endif %}

)

, unioned_labs as (

    select
          person_id
        , data_source
        , code_type
        , code
        , result_date
        , result
    from numeric_egfr_labs

    union all

    select
          person_id
        , data_source
        , code_type
        , code
        , result_date
        , clean_result as result
    from clean_non_numeric_egfr_labs
    where clean_result is not null

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(data_source as {{ dbt.type_string() }}) as data_source
        , cast(code_type as {{ dbt.type_string() }}) as code_type
        , cast(code as {{ dbt.type_string() }}) as code
        , cast(result_date as date) as result_date
        , cast(result as {{ dbt.type_numeric() }}) as result
    from unioned_labs

)

select
      person_id
    , data_source
    , code_type
    , code
    , result_date
    , result
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
