{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
   )
}}

-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


with all_providers_in_claims_dataset as (
select distinct facility_npi as npi, data_source
from {{ ref('core_stage_claims__medical_claim') }}

union all

select distinct rendering_npi as npi, data_source
from {{ ref('core_stage_claims__medical_claim') }}

union all

select distinct billing_npi as npi, data_source
from {{ ref('core_stage_claims__medical_claim') }}
),


provider as (
select aa.*, bb.data_source
from {{ ref('terminology__provider') }} aa
inner join all_providers_in_claims_dataset bb
on aa.npi = bb.npi
)



select 
    npi as practitioner_id
    , npi
    , provider_first_name
    , provider_last_name
    , parent_organization_name as practice_affiliation
    , primary_specialty_description as specialty
    , null as sub_specialty
    , data_source as data_source
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from provider
