{{ config(
     enabled = var('claims_enabled', var('clinical_enabled', False)) | as_bool
   )
}}
select
      person_id
    , sex
    , birth_date
    , death_date
from {{ ref('core__patient') }}
