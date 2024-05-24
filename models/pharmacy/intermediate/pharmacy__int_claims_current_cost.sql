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
        then sum(paid_amount) / sum(quantity) 
      else 0 
    end as cost_per_unit
from {{ ref('pharmacy__stg_pharmacy_claim') }} as p
left join {{ ref('terminology__rxnorm_brand_generic') }} as r 
  on p.rxcui = r.product_rxcui
left join {{ ref('pharmacy__int_brand_with_generic_available') }} as ga 
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
