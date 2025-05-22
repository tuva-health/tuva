{{ config(
     enabled = var('fhir_preprocessing_enabled',var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))) | as_bool
   )
}}
select
      cast(person_id as {{ dbt.type_string() }} ) as patient_internal_id
    , cast(first_name as {{ dbt.type_string() }} ) as name_first
    , cast(last_name as {{ dbt.type_string() }} ) as name_last
    , cast(sex as {{ dbt.type_string() }} ) as gender
    , case
        when race is null then 'UNK'
        else cast(race as {{ dbt.type_string() }} )
      end as race
    , cast(birth_date as date) as birth_date
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
from {{ ref('fhir_preprocessing__stg_core__patient') }}
