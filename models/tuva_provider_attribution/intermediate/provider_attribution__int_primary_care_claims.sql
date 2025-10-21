{{ config(
     enabled = var('provider_attribution_enabled', var('claims_enabled', var('tuva_marts_enabled', True))) | as_bool
   )
}}

-- Primary care services from input_layer medical_claim, joined to member months and provider bucket

with claim_month as (
  select
      mc.person_id
    , mc.claim_id
    , mc.claim_line_number
    , cast(cm.encounter_id as {{ dbt.type_string() }}) as encounter_id
    , mc.claim_start_date
    , mc.claim_end_date
    , cal.year as claim_year
    , cal.month as claim_month
    , cal.year_month_int as claim_year_month_int
    , cast(cal.year_month_int as {{ dbt.type_string() }}) as claim_year_month
    -- Fallback to paid_amount when allowed_amount is absent; many payers omit allowed values.
    , coalesce(nullif(mc.allowed_amount, 0), mc.paid_amount, 0) as allowed_amount
    , cast(mc.rendering_npi as {{ dbt.type_string() }}) as provider_id
    , mc.hcpcs_code
  from {{ ref('provider_attribution__stg_input_layer__medical_claim') }} as mc
  left outer join {{ ref('provider_attribution__stg_core__claims_medical_claim') }} as cm
    on mc.claim_id = cm.claim_id
   and mc.claim_line_number = cm.claim_line_number
   and mc.data_source = cm.data_source
  left outer join {{ ref('provider_attribution__stg_reference_data__calendar') }} as cal
    on cast(mc.claim_start_date as date) = cal.full_date
)

, eligible_claims as (
  select c.*
  from claim_month as c
  inner join {{ ref('provider_attribution__stg_core__member_months') }} as mm
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
    , e.claim_year
    , e.claim_month
    , e.claim_year_month
    , e.claim_year_month_int
    , e.allowed_amount
    , e.provider_id
    , e.hcpcs_code
  from eligible_claims as e
  inner join {{ ref('cms_provider_attribution__primary_care_hcpcs_codes') }} as pc
    on e.hcpcs_code = pc.hcpcs_code
)

, with_bucket as (
  select
      pcc.*
    , pc.provider_bucket
    , pc.prov_specialty
  from primary_care_claims as pcc
  left outer join {{ ref('provider_attribution__provider_classification') }} as pc
    on pcc.provider_id = pc.provider_id
)

select * from with_bucket
