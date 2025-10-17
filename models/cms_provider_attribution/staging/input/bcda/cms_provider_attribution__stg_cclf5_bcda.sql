select distinct
    pat_id.value as person_id
  , eob.identifier_0_value as claim_id -- NOTE: Different from CUR_CLM_UNIQ_ID in CCLF since that is unique to CCLF
  , ln.anchor_sequence as claim_line_number
  , care.qualification_coding_prvdr_spclty_code as clm_prvdr_spclty_cd
  , prcsg.valuecoding_code as clm_prcsg_ind_cd
  , dnl.valuecoding_code as clm_carr_pmt_dnl_cd  
from {{ ref('cms_provider_attribution__stg_eob_header') }} eob
inner join {{ ref('cms_provider_attribution__stg_patient_identifier') }} pat_id
  on replace(eob.patient_reference,'Patient/','') = pat_id.patient_id
  and pat_id.system = 'http://hl7.org/fhir/sid/us-mbi' 
left join {{ ref('cms_provider_attribution__stg_eob_lineitems') }} ln
  on  eob.id  = ln.header_id
left join {{ ref('cms_provider_attribution__stg_eob_item_0_extension') }} prcsg
    on eob.id = prcsg.eob_id
    and prcsg.url = 'https://bluebutton.cms.gov/resources/variables/line_prcsg_ind_cd'
left join {{ ref('cms_provider_attribution__stg_eob_extension') }} dnl
  on eob.id = dnl.eob_id
  and dnl.url = 'https://bluebutton.cms.gov/resources/variables/carr_clm_pmt_dnl_cd'
left join {{ref('cms_provider_attribution__stg_eob_careteam')}} care
  on eob.id = care.eob_id
  and qualification_coding_prvdr_spclty_system = 'https://bluebutton.cms.gov/resources/variables/prvdr_spclty'