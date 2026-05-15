
{{ config(
     enabled = var('claims_enabled', False) | as_bool
   )
}}

-- *************************************************
-- This dbt model creates the provider table 
-- in core. It includes data about all providers
-- present in the raw claims dataset.
-- *************************************************


with all_providers_in_claims_dataset as (
select distinct
    facility_npi as npi
    , data_source
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    rendering_npi as npi
    , data_source
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    billing_npi as npi
    , data_source
from {{ ref('core__stg_claims_medical_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    prescribing_provider_id as npi
    , data_source
from {{ ref('core__stg_claims_pharmacy_claim') }}

{% if target.type == 'fabric' %}
union
{% else %}
union distinct
{% endif %}

select distinct
    dispensing_provider_id as npi
    , data_source
from {{ ref('core__stg_claims_pharmacy_claim') }}
)

, provider_sources as (
select
    npi
    , {{ dbt.listagg(
        measure="distinct data_source",
        delimiter_text="', '",
        order_by_clause="order by data_source"
      ) }} as data_source
from all_providers_in_claims_dataset
group by npi
)

, provider as (
select
    aa.*
    , bb.data_source
from {{ ref('provider_data__provider') }} as aa
inner join provider_sources as bb
on aa.npi = bb.npi
where lower(aa.entity_type_description) = 'individual'
)



select
    cast(npi as {{ dbt.type_string() }}) as practitioner_id
    , cast(npi as {{ dbt.type_string() }}) as npi
    , cast(provider_first_name as {{ dbt.type_string() }}) as first_name
    , cast(provider_last_name as {{ dbt.type_string() }}) as last_name
    , cast(parent_organization_name as {{ dbt.type_string() }}) as practice_affiliation
    , cast(primary_specialty_description as {{ dbt.type_string() }}) as specialty
    , cast(null as {{ dbt.type_string() }}) as sub_specialty
    , cast(data_source as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from provider
