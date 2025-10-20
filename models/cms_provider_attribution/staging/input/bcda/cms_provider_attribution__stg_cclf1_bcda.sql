{{ config(
     enabled = var('claims_preprocessing_enabled', False) and var('attribution_claims_source') == 'bcda'
 | as_bool)
}}

select
    pat_id.value as person_id
  , header.identifier_0_value as claim_id -- NOTE: Different from CUR_CLM_UNIQ_ID in CCLF since that is unique to CCLF
  , header.type_coding_0_code as clm_type_cd
  , othr.provider_identifier_value as othr_prvdr_npi_num
  , nullif(atnd.provider_identifier_value, '~') as atndg_prvdr_npi_num
  , nullif(asst.provider_identifier_value, '~') as oprtg_prvdr_npi_num
  , nullif(eob.contained_0_identifier_0_value, '~') as ccn
  , nullif(ext.valuecoding_code,'N') as clm_mdcr_npmt_rsn_cd
from {{ ref('cms_provider_attribution__stg_eob_header') }} header
inner join {{ ref('cms_provider_attribution__stg_eob') }} eob
  on header.id = eob.id
  and eob.contained_0_identifier_0_type_coding_0_system = 'http://terminology.hl7.org/CodeSystem/v2-0203'
  and eob.contained_0_identifier_0_type_coding_0_code = 'PRN'
inner join {{ ref('cms_provider_attribution__stg_patient_identifier') }} pat_id
  on replace(eob.patient_reference,'Patient/','') = pat_id.patient_id
  and pat_id.system = 'http://hl7.org/fhir/sid/us-mbi' 
  and pat_id.type_coding_0_extension_0_valuecoding_code = 'current'
left join {{ ref('cms_provider_attribution__stg_eob_extension') }} ext
  on eob.id = ext.eob_id
  and ext.url = 'https://bluebutton.cms.gov/resources/variables/clm_mdcr_non_pmt_rsn_cd'
left join {{ ref('cms_provider_attribution__stg_eob_careteam') }} othr
  on  eob.id = othr.eob_id
  and othr.role_coding_0_code = 'otheroperating'
left join {{ ref('cms_provider_attribution__stg_eob_careteam') }} atnd
  on  eob.id = atnd.eob_id
  and atnd.role_coding_0_code = 'attending'
left join {{ ref('cms_provider_attribution__stg_eob_careteam') }} asst
  on  eob.id = asst.eob_id
  and asst.role_coding_0_code = 'assist'
where header.type_coding_0_system = 'https://bluebutton.cms.gov/resources/variables/nch_clm_type_cd'