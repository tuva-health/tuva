{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


with all_providers_in_claims_dataset as (
select distinct facility_npi as npi
from {{ ref('core__medical_claim') }}

union all

select distinct rendering_npi as npi
from {{ ref('core__medical_claim') }}

union all

select distinct billing_npi as npi
from {{ ref('core__medical_claim') }}
),


provider as (
select aa.*
from {{ ref('terminology__provider') }} aa
inner join all_providers_in_claims_dataset bb
on aa.npi = bb.npi
)



select *, '{{ var('last_update')}}' as last_update
from provider
