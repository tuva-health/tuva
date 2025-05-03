{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

/* for each brand rxcui, what is the average cost/unit of all generics available*/

select
    g.product_rxcui as brand_rxcui
  , cl.data_source
  , cl.ndc_code as generic_ndc_code
  , cl.paid_amount
  , cl.claim_count
  , cl.total_units
  , cl.cost_per_unit
  , case when claim_count > 0 then 1 else 0 end as prescribed_atleast_one_generic_history
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__rxnorm_generic_available') }} as g
left outer join {{ ref('pharmacy__int_claims_current_cost') }} as cl
  on cl.ndc_code = g.ndc
where cl.brand_vs_generic = 'generic'
