select distinct
  med.claim_id
  , med.claim_type
  , med.data_source
  , prov.npi as normalized_rendering_npi
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__provider') }} prov
    on med.rendering_npi = prov.npi
where prov.entity_type_description = 'Individual'

union all

select distinct
  med.claim_id
  , med.claim_type
  , med.data_source
  , prov.npi as normalized_billing_npi
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__provider') }} prov
    on med.biling_npi = prov.npi
where med.claim_type = 'professional'
and prov.entity_type_description = 'Individual'

union all

select distinct
  med.claim_id
  , med.claim_type
  , med.data_source
  , prov.npi as normalized_
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__provider') }} prov
    on med.biling_npi = prov.npi
where med.claim_type = 'insititutional'
and prov.entity_type_description = 'Organization'