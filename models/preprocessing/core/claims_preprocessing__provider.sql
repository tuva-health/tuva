

{{ config(
     enabled = var('claims_preprocessing_enabled',var('tuva_packages_enabled',True))
   )
}}




-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


with all_providers_in_claims_dataset as (
select distinct facility_npi as npi
from {{ ref('claims_preprocessing__medical_claim_enhanced') }}

union all

select distinct rendering_npi as npi
from {{ ref('claims_preprocessing__medical_claim_enhanced') }}

union all

select distinct billing_npi as npi
from {{ ref('claims_preprocessing__medical_claim_enhanced') }}
),


provider as (
select aa.*
from {{ ref('terminology__provider') }} aa
inner join all_providers_in_claims_dataset bb
on aa.npi = bb.npi
)



select *
from provider
