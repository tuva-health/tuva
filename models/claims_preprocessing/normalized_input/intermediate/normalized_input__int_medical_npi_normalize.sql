{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select distinct
  med.claim_id
  , med.claim_line_number
  , med.claim_type
  , med.data_source
  , rend_prov.npi as normalized_rendering_npi
  , bill_prov.npi as normalized_billing_npi
  , fac_prov.npi as normalized_facility_npi
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__provider') }} rend_prov
    on med.rendering_npi = rend_prov.npi
    and rend_prov.entity_type_description = 'Individual'
left join {{ ref('terminology__provider') }} bill_prov
    on med.billing_npi = bill_prov.npi
left join {{ ref('terminology__provider') }} fac_prov
    on med.facility_npi = fac_prov.npi
    and fac_prov.entity_type_description = 'Organization'
    and med.claim_type = 'institutional'