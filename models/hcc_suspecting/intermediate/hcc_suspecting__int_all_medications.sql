{{ config(
     enabled = var('hcc_suspecting_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}

with medications as (

    select
          person_id
        , dispensing_date
        , source_code
        , source_code_type
        , ndc_code
        , rxnorm_code
        , data_source
    from {{ ref('hcc_suspecting__stg_core__medication') }}

)

, pharmacy_claims as (

    select
          person_id
        , coalesce(dispensing_date, paid_date) as dispensing_date
        , ndc_code as drug_code
        , 'ndc' as code_system
        , data_source
    from {{ ref('hcc_suspecting__stg_core__pharmacy_claim') }}

)

, ndc_medications as (

    select
          person_id
        , dispensing_date
        , ndc_code as drug_code
        , 'ndc' as code_system
        , data_source
    from medications
    where ndc_code is not null

    union all

    select
          person_id
        , dispensing_date
        , source_code as drug_code
        , 'ndc' as code_system
        , data_source
    from medications
    where lower(source_code_type) = 'ndc'

)

, rxnorm_medications as (

    select
          person_id
        , dispensing_date
        , rxnorm_code as drug_code
        , 'rxnorm' as code_system
        , data_source
    from medications
    where rxnorm_code is not null

    union all

    select
          person_id
        , dispensing_date
        , source_code as drug_code
        , 'rxnorm' as code_system
        , data_source
    from medications
    where lower(source_code_type) = 'rxnorm'

)

, unioned as (

    select * from pharmacy_claims
    union all
    select * from ndc_medications
    union all
    select * from rxnorm_medications

)

, add_data_types as (

    select
          cast(person_id as {{ dbt.type_string() }}) as person_id
        , cast(dispensing_date as date) as dispensing_date
        , cast(drug_code as {{ dbt.type_string() }}) as drug_code
        , cast(code_system as {{ dbt.type_string() }}) as code_system
        , cast(data_source as {{ dbt.type_string() }}) as data_source
    from unioned

)

select
      person_id
    , dispensing_date
    , drug_code
    , code_system
    , data_source
    , '{{ var('tuva_last_run') }}' as tuva_last_run
from add_data_types
