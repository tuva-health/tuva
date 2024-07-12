{{ config(
     enabled = var('claims_enabled', var('tuva_marts_enabled', False)) | as_bool
)}}

select
    p.claim_id
  , p.claim_line_number
  , p.patient_id
  , p.data_source
  , p.patient_id || '|' || p.data_source as patient_source_key
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
  , n.rxcui
  , n.rxnorm_description
  , r.brand_name
  , r.brand_vs_generic
  , r.ingredient_name
  , a.atc_1_name
  , a.atc_2_name
  , a.atc_3_name
  , a.atc_4_name
  , e.generic_available
  , e.generic_available_total_opportunity
  , e.generic_available_sk
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from {{ ref('core__pharmacy_claim') }} as p
left join {{ ref('terminology__ndc') }} as n
  on p.ndc_code = n.ndc
left join {{ ref('pharmacy__pharmacy_claim_expanded') }} as e
  on p.claim_id = e.claim_id
  and p.claim_line_number = e.claim_line_number
left join {{ ref('terminology__rxnorm_brand_generic') }} as r
  on n.rxcui = r.product_rxcui
left join {{ ref('terminology__rxnorm_to_atc') }} as a
  on n.rxcui = a.rxcui
left join {{ ref('core__practitioner') }} prac on p.prescribing_provider_id = prac.practitioner_id
