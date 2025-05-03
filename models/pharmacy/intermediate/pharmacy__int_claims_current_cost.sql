{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}
/* All claims, current cost*/

select
    p.ndc_code
  , p.ndc_description
  , p.data_source
  , p.rxcui
  , r.brand_vs_generic
  , case
      when ga.brand_with_generic_available is not null
        then 'brand_with_generic_available'
      else null
    end as generic_available
  , sum(paid_amount) as paid_amount
  , count(distinct claim_id) as claim_count
  , sum(paid_amount) / count(distinct claim_id) as cost_per_claim
  , sum(quantity) as total_units
  , case
      when sum(quantity) > 0
      and sum(paid_amount) > 0
      then sum(paid_amount) / sum(quantity)
      else null
    end as cost_per_unit
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__stg_pharmacy_claim') }} as p
left outer join {{ ref('terminology__rxnorm_brand_generic') }} as r
  on p.rxcui = r.product_rxcui
left outer join {{ ref('pharmacy__int_brand_with_generic_available') }} as ga
  on p.rxcui = ga.brand_with_generic_available
where p.ndc_code is not null
group by
    case
      when ga.brand_with_generic_available is not null
        then 'brand_with_generic_available'
      else null
    end
  , r.brand_vs_generic
  , p.ndc_code
  , p.rxcui
  , p.ndc_description
  , p.data_source
