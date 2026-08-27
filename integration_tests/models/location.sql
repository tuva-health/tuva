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
    , 'location' as x_tuva_test_extension
    , 'location' as ext_tuva_test_extension
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__location') }}
