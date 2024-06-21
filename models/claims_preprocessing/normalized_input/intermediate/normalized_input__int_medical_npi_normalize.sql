{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}


select distinct
  med.claim_id
  , med.claim_line_number
  , med.claim_type
  , med.data_source
  , rend_prov.npi as normalized_rendering_npi
  , cast(coalesce(rend_prov.provider_last_name||', '|| rend_prov.provider_first_name, rend_prov.provider_organization_name) as {{ dbt.type_string() }} ) as normalized_rendering_name
  , bill_prov.npi as normalized_billing_npi
  , cast(coalesce(bill_prov.provider_last_name||', '|| bill_prov.provider_first_name, bill_prov.provider_organization_name) as {{ dbt.type_string() }} ) as normalized_billing_name
  , fac_prov.npi as normalized_facility_npi
  , cast(coalesce(fac_prov.provider_last_name||', '|| fac_prov.provider_first_name, fac_prov.provider_organization_name) as {{ dbt.type_string() }} ) as normalized_facility_name
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