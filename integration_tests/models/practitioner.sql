{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

select
      practitioner_id
    , npi
    , first_name
    , last_name
    , practice_affiliation
    , specialty
    , sub_specialty
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__practitioner') }}
