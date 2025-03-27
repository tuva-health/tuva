
{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

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
where lower(aa.entity_type_description) = 'organization'
)



select
    cast(npi as {{ dbt.type_string() }}) as location_id
    , cast(npi as {{ dbt.type_string() }}) as npi
    , cast(provider_organization_name as {{ dbt.type_string() }}) as name
    , cast(null as {{ dbt.type_string() }}) as facility_type
    , cast(parent_organization_name as {{ dbt.type_string() }}) as parent_organization
    , cast(practice_address_line_1 as {{ dbt.type_string() }}) as address
    , cast(practice_city as {{ dbt.type_string() }}) as city
    , cast(practice_state as {{ dbt.type_string() }}) as state
    , cast(practice_zip_code as {{ dbt.type_string() }}) as zip_code
    , cast(null as {{ dbt.type_float() }}) as latitude
    , cast(null as {{ dbt.type_float() }}) as longitude
    , cast(null as {{ dbt.type_string() }}) as data_source
    , cast('{{ var('tuva_last_run') }}' as {{ dbt.type_timestamp() }}) as tuva_last_run
from provider
