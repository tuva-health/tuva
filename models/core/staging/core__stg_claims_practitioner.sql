-- depends_on: {{ ref('data_quality__claims_preprocessing_summary') }}

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
where lower(aa.entity_type_description) = 'individual'
)



select 
    cast(npi as {{ dbt.type_string() }} ) as practitioner_id
    , cast(npi as {{ dbt.type_string() }} ) as npi
    , cast(provider_first_name as {{ dbt.type_string() }} ) as provider_first_name
    , cast(provider_last_name as {{ dbt.type_string() }} ) as provider_last_name
    , cast(parent_organization_name as {{ dbt.type_string() }} ) as practice_affiliation
    , cast(primary_specialty_description as {{ dbt.type_string() }} ) as specialty
    , cast(null as {{ dbt.type_string() }} ) as sub_specialty
    , cast(data_source as {{ dbt.type_string() }} ) as data_source
    , cast('{{ var('tuva_last_run')}}' as {{ dbt.type_timestamp() }} ) as tuva_last_run
from provider
