{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

/* for each brand rxcui, what is the average cost/unit of generics available*/

select
    g.product_rxcui as brand_rxcui
  , cl.data_source
  , max(case when claim_count > 0 then 1 else 0 end) as prescribed_atleast_one_generic_history
  , sum(case when total_units > 0 and paid_amount > 0 then paid_amount else null end) / sum(case when total_units > 0 and paid_amount > 0 then total_units else null end) as generic_average_cost_per_unit
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__rxnorm_generic_available') }} as g
left outer join {{ ref('pharmacy__int_claims_current_cost') }} as cl
  on cl.ndc_code = g.ndc
where cl.brand_vs_generic = 'generic'
group by
    g.product_rxcui
  , cl.data_source
