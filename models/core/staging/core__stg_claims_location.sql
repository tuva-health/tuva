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
    npi as location_id
    , npi
    , provider_organization_name as name
    , null as facility_type
    , parent_organization_name as parent_organization
    , practice_address_line_1 as address
    , practice_city as city
    , practice_state as state
    , practice_zip_code as zip_code
    , null as latitude
    , null as longitude
    , data_source as data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from provider
