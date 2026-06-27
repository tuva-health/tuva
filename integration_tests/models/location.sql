{{ config(
     enabled = var('clinical_enabled', False) | as_bool
   )
}}

select
      location_id
    , npi
    , name
    , facility_type
    , parent_organization
    , address
    , city
    , state
    , zip_code
    , latitude
    , longitude
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__location') }}
