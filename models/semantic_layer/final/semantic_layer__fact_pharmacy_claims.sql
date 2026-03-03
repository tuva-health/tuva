{{ config(
    enabled = (var('semantic_layer_enabled',False) | as_bool) and (var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool)
) }}

select
    p.claim_id
  , p.claim_line_number
  , p.person_id
  , p.data_source
  , {{ concat_custom(['p.person_id', "'|'", 'p.data_source']) }} as patient_source_key
  , p.ndc_code
  , coalesce(n.fda_description, n.rxnorm_description) as ndc_description
  , p.paid_amount
  , p.allowed_amount
  , p.prescribing_provider_id
  , p.prescribing_provider_name
  , prac.specialty as prescribing_specialty
  , p.dispensing_provider_id
  , p.dispensing_provider_name
  , p.paid_date
  , p.dispensing_date
  , p.days_supply
  , case 
      when p.days_supply = 0 then null
      else p.paid_amount / p.days_supply
    end as cost_per_day
  , case 
      when p.days_supply = 0 then null
      else (p.paid_amount / p.days_supply) * 30
    end as thirty_day_equivalent_cost
  , case 
      when p.days_supply = 0 then 0
      when (p.paid_amount / p.days_supply) * 30 >= 950 then 1
      else 0 
    end as specialty_tier -- $950 is the threshold set by CMS for CY 2024
  , n.rxcui
  , n.rxnorm_description
  , r.brand_name
  , r.brand_vs_generic
  , r.ingredient_name
  , a.atc_1_name
  , a.atc_2_name
  , a.atc_3_name
  , a.atc_4_name
  , pe.generic_available_total_opportunity
  , pe.generic_average_cost_per_unit * p.quantity as generic_cost_at_units
  , pe.brand_cost_per_unit * p.quantity as brand_paid_amount
  , pe.generic_available
  , pe.generic_available_sk
  , p.tuva_last_run
from {{ ref('semantic_layer__stg_core__pharmacy_claim') }} as p
left outer join {{ ref('terminology__ndc') }} as n 
  on p.ndc_code = n.ndc
left outer join {{ ref('terminology__rxnorm_brand_generic') }} as r 
  on n.rxcui = r.product_rxcui
left outer join {{ ref('terminology__rxnorm_to_atc') }} as a 
  on n.rxcui = a.rxcui
left outer join {{ ref('semantic_layer__stg_core__practitioner') }} as prac 
  on p.prescribing_provider_id = prac.practitioner_id
left outer join {{ ref('semantic_layer__stg_pharmacy__pharmacy_claim_expanded') }} as pe 
  on p.data_source = pe.data_source
  and p.claim_id = pe.claim_id
  and p.claim_line_number = pe.claim_line_number
