{{ config(
    enabled = var('brand_generic_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with generic_sk as (
  select
      generic_available_sk
    , claim_id
    , claim_line_number
    , data_source
  from {{ ref('pharmacy__pharmacy_claim_expanded') }} as e
  where generic_available_sk is not null
)

select
    sk.generic_available_sk
  , p.data_source
  , p.ndc_code as brand_ndc_code
  , p.ndc_description as brand_ndc_description
  , p.rxcui as brand_rxcui
  , p.paid_amount as brand_paid_amount
  , p.quantity as brand_units
  , case
      when p.quantity = 0
        then 0
      else p.paid_amount / p.quantity
    end as brand_paid_per_unit
  , ga.ndc as generic_ndc
  , n.fda_description as generic_ndc_description
  , case
      when gc.ndc_code is not null
        then 1
      else 0
    end as generic_prescribed_history
  , gc.cost_per_unit as generic_cost_per_unit
  , gc.cost_per_unit * p.quantity as generic_cost_at_units
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('pharmacy__stg_pharmacy_claim') }} as p
inner join generic_sk as sk
  on p.claim_id = sk.claim_id
  and p.claim_line_number = sk.claim_line_number
  and p.data_source = sk.data_source
inner join {{ ref('pharmacy__int_brand_with_generic_available') }} as b
  on p.rxcui = b.brand_with_generic_available
inner join {{ ref('pharmacy__rxnorm_generic_available') }} as ga
  on p.rxcui = ga.product_rxcui
  and ga.ndc_product_tty in ('SCD', 'GPCK')
left outer join {{ ref('terminology__ndc') }} as n
  on ga.ndc = n.ndc
left outer join {{ ref('pharmacy__int_claims_current_cost') }} as gc
  on ga.ndc = gc.ndc_code
  and gc.brand_vs_generic = 'generic'
  and gc.data_source = p.data_source
where
    ga.product_startmarketingdate is not null
    {% if target.type == 'fabric' %}
        and cast(ga.product_startmarketingdate as date) <= GETDATE()
    {% else %}
        and cast(ga.product_startmarketingdate as date) <= current_date
{% endif %}
