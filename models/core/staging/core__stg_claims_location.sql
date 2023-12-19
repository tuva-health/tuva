-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

with all_providers_in_claims_dataset as (
select distinct facility_npi as npi, data_source
from {{ ref('core__stg_claims_medical_claim') }}

union all

select distinct rendering_npi as npi, data_source
from {{ ref('core__stg_claims_medical_claim') }}

union all

select distinct billing_npi as npi, data_source
from {{ ref('core__stg_claims_medical_claim') }}
),


provider as (
select aa.*, bb.data_source
from {{ ref('terminology__provider') }} aa
inner join all_providers_in_claims_dataset bb
on aa.npi = bb.npi
where lower(aa.entity_type_description) = 'organization'
)



select 
    cast(npi as {{ dbt.type_string() }} ) as location_id
    , cast(npi as {{ dbt.type_string() }} ) as npi
    , cast(provider_organization_name as {{ dbt.type_string() }} ) as name
    , cast(null as {{ dbt.type_string() }} ) as facility_type
    , cast(parent_organization_name as {{ dbt.type_string() }} ) as parent_organization
    , cast(practice_address_line_1 as {{ dbt.type_string() }} ) as address
    , cast(practice_city as {{ dbt.type_string() }} ) as city
    , cast(practice_state as {{ dbt.type_string() }} ) as state
    , cast(practice_zip_code as {{ dbt.type_string() }} ) as zip_code
    , cast(null as {{ dbt.type_float() }} ) as latitude
    , cast(null as {{ dbt.type_float() }} ) as longitude
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast( '{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from provider
