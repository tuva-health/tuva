{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

-- Map NPI to specialty description and CMS assignment bucket (pcp/specialist/npp)
with base as (
  select 
      cast(p.npi as {{ dbt.type_string() }}) as provider_id
    , p.primary_taxonomy_code
    , p.primary_specialty_description as prov_specialty
    , lower(p.entity_type_description) as entity_type
  from {{ ref('provider_attribution__stg_terminology__provider') }} p
)

, mapped as (
  select 
      b.provider_id
    , b.prov_specialty
    , case 
        when lower(a.primary_care_physician_step1) = 'yes' and a.physician = 1 then 'pcp'
        when lower(a.specialist_physician_step_2) = 'yes' and a.physician = 1 then 'specialist'
        when a.physician = 0 then 'npp'
      end as provider_bucket
  from base b
  inner join {{ ref('terminology__medicare_provider_and_supplier_taxonomy_crosswalk') }} x
    on cast(b.primary_taxonomy_code as {{ dbt.type_string() }}) = cast(x.provider_taxonomy_code as {{ dbt.type_string() }})
  inner join {{ ref('cms_provider_attribution__provider_specialty_assignment_codes') }} a
    on cast(x.medicare_specialty_code as {{ dbt.type_string() }}) = cast(a.specialty_code as {{ dbt.type_string() }})
  where b.entity_type = 'individual'
)

select 
    provider_id
  , prov_specialty
  , provider_bucket
from mapped
