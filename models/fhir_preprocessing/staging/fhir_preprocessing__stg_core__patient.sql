{{ config(
     enabled = var('fhir_preprocessing_enabled',False) | as_bool
   )
}}
select
      person_id
    , first_name
    , last_name
    , sex
    , race
    , birth_date
    , data_source
from {{ ref('core__patient') }}
