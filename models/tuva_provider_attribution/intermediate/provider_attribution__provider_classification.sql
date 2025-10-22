{{ config(
     enabled = var('tuva_provider_attribution', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
   )
}}

-- Map NPI to specialty description and CMS assignment bucket (pcp/specialist/npp)
with base as (
  select
      cast(p.npi as {{ dbt.type_string() }}) as provider_id
    , p.primary_taxonomy_code
    , p.primary_specialty_description as prov_specialty
    , lower(trim(p.entity_type_description)) as entity_type
  from {{ ref('provider_attribution__stg_terminology__provider') }} as p
)

, mapped as (
  select
      b.provider_id
    , b.prov_specialty
    , case
        when lower(a.primary_care_physician_step1) = 'yes' and a.physician = 1 then 'pcp'
        when lower(a.specialist_physician_step_2) = 'yes' and a.physician = 1 then 'specialist'
        when a.physician = 0 then 'npp'
        else 'unknown'
      end as provider_bucket
  from base as b
  inner join {{ ref('terminology__medicare_provider_and_supplier_taxonomy_crosswalk') }} as x
    on trim(cast(b.primary_taxonomy_code as {{ dbt.type_string() }})) = trim(cast(x.provider_taxonomy_code as {{ dbt.type_string() }}))
  inner join {{ ref('cms_provider_attribution__provider_specialty_assignment_codes') }} as a
    on right(concat('00', trim(x.medicare_specialty_code)), 2)
     = right(concat('00', trim(a.specialty_code)), 2)
  where b.entity_type = 'individual'
)

, rnk as (
  select
      provider_id
    , prov_specialty
    , provider_bucket
    , row_number() over (partition by provider_id
order by bucket_priority) as bucket_rank
  from (
    select
        mapped.*
      , case provider_bucket
          when 'pcp' then 1
          when 'npp' then 2
          when 'specialist' then 3
          else 4
        end as bucket_priority
    from mapped
  ) as prioritized
)

select
    provider_id
  , prov_specialty
  , provider_bucket
from rnk
where bucket_rank = 1
