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
    , 'practitioner' as x_tuva_test_extension
    , 'practitioner' as ext_tuva_test_extension
    , cast(null as {{ dbt.type_timestamp() }}) as ingest_datetime
    , data_source
from {{ ref('the_tuva_project', 'synthetic_data__practitioner') }}
