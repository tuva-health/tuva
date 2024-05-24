select 
    p.ndc_code
  , n.fda_description as ndc_description
  , p.data_source
  , n.rxcui
  , p.paid_amount
  , p.claim_id
  , p.claim_line_number
  , p.patient_id
  , p.member_id
  , p.prescribing_provider_npi
  , p.dispensing_provider_npi
  , p.dispensing_date
  , p.quantity
  , p.days_supply
  , p.refills
  , p.paid_date
from {{ ref('core__pharmacy_claim') }} as p
left join {{ ref('terminology__ndc') }} as n 
  on p.ndc_code = n.ndc
