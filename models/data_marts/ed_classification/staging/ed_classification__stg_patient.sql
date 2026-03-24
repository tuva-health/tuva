{{ config(
     enabled = var('claims_enabled', False)
 | as_bool
   )
}}

select
    person_id
    , sex
    , birth_date
    , race
    , state
    , zip_code
    , latitude
    , longitude
    , data_source
from {{ ref('core__patient') }}
