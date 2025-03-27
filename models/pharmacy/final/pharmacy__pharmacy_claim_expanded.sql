{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with all_drugs as (
  select
      p.data_source
    , p.claim_id
    , p.claim_line_number
    , p.person_id
    , p.member_id
    , p.prescribing_provider_id
    , p.dispensing_provider_id
    , p.dispensing_date
    , p.ndc_code
    , p.ndc_description
    , p.quantity
    , p.days_supply
    , p.refills
    , p.paid_date
    , p.paid_amount
    , p.allowed_amount
    , p.rxcui
    , r.product_name
    , r.product_tty
    , r.brand_vs_generic
    , r.brand_name
    , r.clinical_product_rxcui as generic_rxcui
    , r.clinical_product_name as generic_rxcui_description
    , r.clinical_product_tty as generic_tty
    , r.ingredient_name
    , r.dose_form_name
    , case
        when ga.brand_with_generic_available is not null
          then 'brand_with_generic_available'
        else r.brand_vs_generic
      end as generic_available
    , opp.brand_cost_per_unit
    , opp.generic_average_cost_per_unit
    , opp.brand_less_generic_cost_per_unit
    , opp.generic_available_total_opportunity
  , '{{ var('tuva_last_run') }}' as tuva_last_run
  from {{ ref('pharmacy__stg_pharmacy_claim') }} as p
  left outer join {{ ref('terminology__rxnorm_brand_generic') }} as r
    on p.rxcui = r.product_rxcui
  left outer join {{ ref('pharmacy__int_brand_with_generic_available') }} as ga
    on p.rxcui = ga.brand_with_generic_available
  left outer join {{ ref('pharmacy__brand_generic_opportunity') }} as opp
    on p.claim_id = opp.claim_id
    and p.claim_line_number = opp.claim_line_number
    and p.data_source = opp.data_source
)

, generic_available as (
  select
      *
    , row_number() over (
order by ndc_code, data_source) as generic_available_sk
  from all_drugs
  where generic_available = 'brand_with_generic_available'
)

select
    a.data_source
  , a.claim_id
  , a.claim_line_number
  , a.person_id
  , a.prescribing_provider_id
  , a.dispensing_provider_id
  , a.dispensing_date
  , a.ndc_code
  , a.ndc_description
  , a.quantity
  , a.days_supply
  , a.refills
  , a.paid_date
  , a.paid_amount
  , a.allowed_amount
  , a.rxcui
  , a.product_name
  , a.product_tty
  , a.brand_vs_generic
  , a.brand_name
  , a.generic_rxcui
  , a.generic_rxcui_description
  , a.generic_tty
  , a.ingredient_name
  , a.dose_form_name
  , a.generic_available
  , a.brand_cost_per_unit
  , a.generic_average_cost_per_unit
  , a.brand_less_generic_cost_per_unit
  , a.generic_available_total_opportunity
  , g.generic_available_sk
from all_drugs as a
left outer join generic_available as g
  on a.claim_id = g.claim_id
  and a.claim_line_number = g.claim_line_number
  and a.data_source = g.data_source
