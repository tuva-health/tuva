{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


select distinct
  med.claim_id
  , med.claim_line_number
  , med.claim_type
  , med.data_source
  , cal_claim_start.full_date as normalized_claim_start_date
  , cal_claim_end.full_date as normalized_claim_end_date
  , cal_claim_line_start.full_date as normalized_claim_line_start_date
  , cal_claim_line_end.full_date as normalized_claim_line_end_date
  , cal_admission.full_date as normalized_admission_date
  , cal_discharge.full_date as normalized_discharge_date
  , '{{ var('tuva_last_run')}}' as tuva_last_run
from {{ ref('normalized_input__stg_medical_claim') }} med
left join {{ ref('terminology__calendar') }} cal_claim_start
    on med.claim_start_date = cal_claim_start.full_date
left join {{ ref('terminology__calendar') }} cal_claim_end
    on med.claim_end_Date = cal_claim_end.full_date
left join {{ ref('terminology__calendar') }} cal_claim_line_start
    on med.claim_line_start_date = cal_claim_line_start.full_date
left join {{ ref('terminology__calendar') }} cal_claim_line_end
    on med.claim_line_end_date = cal_claim_line_end.full_date
left join {{ ref('terminology__calendar') }} cal_admission
    on med.admission_date = cal_admission.full_date
left join {{ ref('terminology__calendar') }} cal_discharge
    on med.discharge_date = cal_discharge.full_date
