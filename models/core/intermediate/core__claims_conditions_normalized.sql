
{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',var('tuva_marts_enabled',False)))
 | as_bool
   )
}}



select
  aa.claim_id,
  aa.data_source,
  aa.recorded_date,
  aa.person_id,
  aa.member_id,
  aa.condition_rank,
  aa.diagnosis_code_type as source_code_type, 
  aa.source_code,
  aa.present_on_admit_code,
  aa.diagnosis_code_type as normalized_code_type,  
  case
    when source_code_type = 'icd-9-cm' then bb.icd_9_cm
    when source_code_type = 'icd-10-cm' then cc.icd_10_cm
    else null
  end as normalized_code,
  case
    when source_code_type = 'icd-9-cm' then bb.long_description
    when source_code_type = 'icd-10-cm' then cc.long_description
    else null
  end as normalized_description,
  dd.present_on_admit_description
  
from {{ ref('core__claims_conditions_long') }} aa

left join {{ ref('terminology__icd_9_cm') }} bb
on replace(aa.source_code,'.','') = bb.icd_9_cm

left join {{ ref('terminology__icd_10_cm') }} cc 
on replace(aa.source_code,'.','') = cc.icd_10_cm

left join {{ ref('terminology__present_on_admission') }} dd 
on aa.present_on_admit_code = dd.present_on_admit_code
