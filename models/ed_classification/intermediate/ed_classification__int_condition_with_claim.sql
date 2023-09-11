/*
Denormalized view of each condition row with additional provider and patient level
information merged on based on the header level detail on the claim
*/

{{ config(enabled=var('ed_classification_enabled',var('tuva_packages_enabled',True))) }}

with deduped_claims as (
   select distinct
      claim_id
      , claim_line_end_date
      , service_category_1
      , service_category_2
      , facility_npi
      , billing_npi
      , patient_id
   from {{ var('medical_claim') }} mc
   where mc.claim_line_number = 1
)
, deduped_providers as (
   select distinct
      npi
      , entity_type_code
      , entity_type_description
      , primary_taxonomy_code
      , primary_specialty_description
      , provider_name
      , parent_organization_name
      , practice_city
      , practice_state
      , practice_zip_code
   from {{ var('provider') }}
   where deactivation_flag = 0
)

select
  c.claim_id
  , c.patient_id
  , c.condition_date
  , c.code
  , c.description
  , c.ccs_description_with_covid
  , c.condition_date_year
  , c.condition_date_year_month
  , c.claim_paid_amount_sum
  , c.classification
  -- claim level additions
  , mc.service_category_1
  , mc.service_category_2
  -- provider level additions
  , coalesce(fp.parent_organization_name, bp.parent_organization_name, fp.provider_name, bp.provider_name) as provider_parent_organization_name_with_provider_name
  , coalesce(fp.provider_name, bp.provider_name) as provider_name
  , coalesce(fp.parent_organization_name, bp.parent_organization_name) as provider_parent_organization_name
  , coalesce(fp.practice_state, bp.practice_state) as provider_practice_state
  , coalesce(fp.practice_zip_code, bp.practice_zip_code) as provider_practice_zip_code
  -- patient level additions
  , p.gender as patient_gender
  , p.birth_date as patient_birth_date
  , {{ dbt_date.periods_since('patient_birth_date', period_name='year') }} as patient_age
  , p.race as patient_race
  , p.state as patient_state

from {{ ref('ed_classified_condition_with_class') }} c
inner join deduped_claims mc
      on c.claim_id = mc.claim_id
      and c.condition_date = mc.claim_line_end_date
left join deduped_providers fp on mc.facility_npi = fp.npi
left join deduped_providers bp on mc.billing_npi = bp.npi
left join {{ var('patient') }} p on mc.patient_id = p.patient_id
