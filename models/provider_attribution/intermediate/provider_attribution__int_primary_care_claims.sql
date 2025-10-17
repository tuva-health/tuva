{{ config(
     enabled = var('provider_attribution_enabled', var('tuva_marts_enabled', True)) | as_bool
   )
}}

-- Primary care services from input_layer medical_claim, joined to member months and provider bucket

with claim_month as (
  select 
      mc.person_id
    , mc.claim_id
    , mc.claim_line_number
    , mc.encounter_id
    , mc.claim_start_date
    , mc.claim_end_date
    , {{ concat_custom([date_part('year', 'mc.claim_start_date'), dbt.right(concat_custom(["'0'", date_part('month','mc.claim_start_date')]), 2)]) }} as claim_year_month
    , coalesce(nullif(mc.allowed_amount, 0), mc.paid_amount, 0) as allowed_amount
    , cast(mc.rendering_npi as {{ dbt.type_string() }}) as provider_id
    , mc.hcpcs_code
  from {{ ref('input_layer__medical_claim') }} mc
)

, eligible_claims as (
  select c.*
  from claim_month c
  inner join {{ ref('core__member_months') }} mm
    on c.person_id = mm.person_id
   and c.claim_year_month = mm.year_month
)

, primary_care_claims as (
  select 
      e.person_id
    , e.claim_id
    , e.claim_line_number
    , e.encounter_id
    , e.claim_start_date
    , e.claim_end_date
    , e.claim_year_month
    , e.allowed_amount
    , e.provider_id
  from eligible_claims e
  inner join {{ ref('cms_provider_attribution__primary_care_hcpcs_codes') }} pc
    on e.hcpcs_code = pc.hcpcs_code
)

, with_bucket as (
  select 
      pcc.*
    , pc.provider_bucket
    , pc.prov_specialty
  from primary_care_claims pcc
  left join {{ ref('provider_attribution__provider_classification') }} pc
    on pcc.provider_id = pc.provider_id
)

select * from with_bucket
