{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the condition table in core.
-- *************************************************

with unpivot_cte as (

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_1 as source_code,
  1 as diagnosis_rank,
  diagnosis_poa_1 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_1 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
	   discharge_date,
	   claim_end_date,
	   claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_2 as source_code,
  2 as diagnosis_rank,
  diagnosis_poa_2 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_2 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_3 as source_code,
  3 as diagnosis_rank,
  diagnosis_poa_3 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_3 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_4 as source_code,
  4 as diagnosis_rank,
  diagnosis_poa_4 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_4 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_5 as source_code,
  5 as diagnosis_rank,
  diagnosis_poa_5 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_5 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_6 as source_code,
  6 as diagnosis_rank,
  diagnosis_poa_6 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_6 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_7 as source_code,
  7 as diagnosis_rank,
  diagnosis_poa_7 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_7 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_8 as source_code,
  8 as diagnosis_rank,
  diagnosis_poa_8 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_8 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_9 as source_code,
  9 as diagnosis_rank,
  diagnosis_poa_9 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_9 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_10 as source_code,
  10 as diagnosis_rank,
  diagnosis_poa_10 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_10 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_11 as source_code,
  11 as diagnosis_rank,
  diagnosis_poa_11 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_11 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_12 as source_code,
  12 as diagnosis_rank,
  diagnosis_poa_12 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_12 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_13 as source_code,
  13 as diagnosis_rank,
  diagnosis_poa_13 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_13 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_14 as source_code,
  14 as diagnosis_rank,
  diagnosis_poa_14 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_14 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_15 as source_code,
  15 as diagnosis_rank,
  diagnosis_poa_15 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_15 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_16 as source_code,
  16 as diagnosis_rank,
  diagnosis_poa_16 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_16 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_17 as source_code,
  17 as diagnosis_rank,
  diagnosis_poa_17 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_17 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_18 as source_code,
  18 as diagnosis_rank,
  diagnosis_poa_18 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_18 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_19 as source_code,
  19 as diagnosis_rank,
  diagnosis_poa_19 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_19 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_20 as source_code,
  20 as diagnosis_rank,
  diagnosis_poa_20 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_20 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_21 as source_code,
  21 as diagnosis_rank,
  diagnosis_poa_21 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_21 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_22 as source_code,
  22 as diagnosis_rank,
  diagnosis_poa_22 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_22 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_23 as source_code,
  23 as diagnosis_rank,
  diagnosis_poa_23 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_23 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_24 as source_code,
  24 as diagnosis_rank,
  diagnosis_poa_24 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_24 is not null

union all 

select
  claim_id,
  patient_id,
  coalesce(admission_date,
           claim_start_date,
           claim_line_start_date,
           discharge_date,
           claim_end_date,
           claim_line_end_date) as condition_date,
  'discharge_diagnosis' as condition_type,
  diagnosis_code_type as source_code_type,
  diagnosis_code_25 as source_code,
  25 as diagnosis_rank,
  diagnosis_poa_25 as present_on_admit_code,
  data_source
from {{ ref('medical_claim') }} 
where diagnosis_code_25 is not null

)

select distinct
    null as condition_id,
    unpivot_cte.patient_id,
    eg.encounter_id,
    unpivot_cte.claim_id,
    unpivot_cte.condition_date as recorded_date,
    null as onset_date,
    null as resolved_date,
    'active' as status,
    unpivot_cte.condition_type as condition_type,
    unpivot_cte.source_code_type as source_code_type,
    unpivot_cte.source_code as source_code,
    null as source_description,
    case
    when icd.icd_10_cm is not null then 'icd-10-cm'
    end as normalized_code_type,
    icd.icd_10_cm as normalized_code,
    icd.description as normalized_description,
    unpivot_cte.diagnosis_rank as condition_rank,
    unpivot_cte.present_on_admit_code as present_on_admit_code,
    poa.present_on_admit_description as present_on_admit_description,
    unpivot_cte.data_source,
    '{{ var('tuva_last_run')}}' as tuva_last_run
from unpivot_cte
left join {{ ref('acute_inpatient__encounter_data_for_medical_claims')}} as eg
    on  unpivot_cte.claim_id = eg.claim_id
    and unpivot_cte.patient_id = eg.patient_id
left join {{ ref('terminology__icd_10_cm') }} icd
    on unpivot_cte.source_code = icd.icd_10_cm
left join {{ ref('terminology__present_on_admission') }} as poa
    on unpivot_cte.present_on_admit_code = poa.present_on_admit_code