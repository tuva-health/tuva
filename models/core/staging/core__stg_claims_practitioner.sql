
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


with all_providers_in_claims_dataset as (
select distinct facility_id as npi
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct rendering_id as npi
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct billing_id as npi
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct prescribing_provider_id as npi
from {{ ref('core__stg_claims_pharmacy_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct dispensing_provider_id as npi
from {{ ref('core__stg_claims_pharmacy_claim') }}
)


, provider as (
select aa.*
from {{ ref('terminology__provider') }} as aa
inner join all_providers_in_claims_dataset as bb
on aa.npi = bb.npi
where lower(aa.entity_type_description) = 'individual'
)



select
    cast(npi as {{ dbt.type_string() }}) as practitioner_id
    , cast(npi as {{ dbt.type_string() }}) as npi
    , cast(provider_first_name as {{ dbt.type_string() }}) as provider_first_name
    , cast(provider_last_name as {{ dbt.type_string() }}) as provider_last_name
    , cast(parent_organization_name as {{ dbt.type_string() }}) as practice_affiliation
    , cast(primary_specialty_description as {{ dbt.type_string() }}) as specialty
    , cast(null as {{ dbt.type_string() }}) as sub_specialty
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from provider
