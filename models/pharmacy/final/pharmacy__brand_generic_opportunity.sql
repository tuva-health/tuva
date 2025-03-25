{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

/* claimid/line/source grain. calculates opportunity at line level*/

with cpu as (
select claim_id
, claim_line_number
, data_source
, case when quantity > 0 then paid_amount / quantity else null end as brand_cost_per_unit
from {{ ref('pharmacy__stg_pharmacy_claim') }}
)


select
    pc.data_source
  , pc.claim_id
  , pc.claim_line_number
  , cc.ndc_code
  , cc.ndc_description
  , cc.rxcui as brand_rxcui
  , cc.brand_vs_generic
  , cc.generic_available
  , pc.paid_amount
  , pc.quantity as total_units
  , cpu.brand_cost_per_unit
  , gc.generic_average_cost_per_unit
  , cpu.brand_cost_per_unit - gc.generic_average_cost_per_unit as brand_less_generic_cost_per_unit
  , case
      when cpu.brand_cost_per_unit - gc.generic_average_cost_per_unit > 0
        then (cpu.brand_cost_per_unit - gc.generic_average_cost_per_unit) * pc.quantity
      else 0
    end as generic_available_total_opportunity
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__stg_pharmacy_claim') }} as pc
inner join cpu on pc.claim_id = cpu.claim_id
  and
  pc.claim_line_number = cpu.claim_line_number
  and
  pc.data_source = cpu.data_source
inner join {{ ref('pharmacy__int_claims_current_cost') }} as cc
  on cc.ndc_code = pc.ndc_code
  and
  cc.data_source = pc.data_source
inner join {{ ref('pharmacy__int_generic_cost') }} as gc
  on cc.rxcui = gc.brand_rxcui
  and gc.data_source = cc.data_source
where prescribed_atleast_one_generic_history = 1
