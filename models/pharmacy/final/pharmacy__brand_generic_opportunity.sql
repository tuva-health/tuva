/* Table includes only brand ndcs that have generics available. calculates opportunity column for each.*/

select 
    cc.data_source
  , cc.ndc_code
  , cc.ndc_description
  , cc.rxcui as brand_rxcui
  , cc.brand_vs_generic 
  , cc.generic_available
  , cc.paid_amount
  , cc.total_units
  , cc.cost_per_unit as brand_cost_per_unit
  , gc.generic_average_cost_per_unit 
  , cc.cost_per_unit - gc.generic_average_cost_per_unit as brand_less_generic_cost_per_unit
  , case 
      when cc.cost_per_unit - gc.generic_average_cost_per_unit > 0 
        then (cc.cost_per_unit - gc.generic_average_cost_per_unit) * cc.total_units 
      else 0 
    end as generic_available_total_opportunity
from {{ ref('pharmacy__int_claims_current_cost') }} as cc
inner join {{ ref('pharmacy__int_generic_cost') }} as gc 
  on cc.rxcui = gc.brand_rxcui
  and gc.data_source = cc.data_source
where prescribed_atleast_one_generic_history = 1

