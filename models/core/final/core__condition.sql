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
  diagnosis_code_type as code_type,
  diagnosis_code_1 as code,
  1 as diagnosis_rank,
  diagnosis_poa_1 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_2 as code,
  2 as diagnosis_rank,
  diagnosis_poa_2 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_3 as code,
  3 as diagnosis_rank,
  diagnosis_poa_3 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_4 as code,
  4 as diagnosis_rank,
  diagnosis_poa_4 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_5 as code,
  5 as diagnosis_rank,
  diagnosis_poa_5 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_6 as code,
  6 as diagnosis_rank,
  diagnosis_poa_6 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_7 as code,
  7 as diagnosis_rank,
  diagnosis_poa_7 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_8 as code,
  8 as diagnosis_rank,
  diagnosis_poa_8 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_9 as code,
  9 as diagnosis_rank,
  diagnosis_poa_9 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_10 as code,
  10 as diagnosis_rank,
  diagnosis_poa_10 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_11 as code,
  11 as diagnosis_rank,
  diagnosis_poa_11 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_12 as code,
  12 as diagnosis_rank,
  diagnosis_poa_12 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_13 as code,
  13 as diagnosis_rank,
  diagnosis_poa_13 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_14 as code,
  14 as diagnosis_rank,
  diagnosis_poa_14 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_15 as code,
  15 as diagnosis_rank,
  diagnosis_poa_15 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_16 as code,
  16 as diagnosis_rank,
  diagnosis_poa_16 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_17 as code,
  17 as diagnosis_rank,
  diagnosis_poa_17 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_18 as code,
  18 as diagnosis_rank,
  diagnosis_poa_18 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_19 as code,
  19 as diagnosis_rank,
  diagnosis_poa_19 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_20 as code,
  20 as diagnosis_rank,
  diagnosis_poa_20 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_21 as code,
  21 as diagnosis_rank,
  diagnosis_poa_21 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_22 as code,
  22 as diagnosis_rank,
  diagnosis_poa_22 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_23 as code,
  23 as diagnosis_rank,
  diagnosis_poa_23 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_24 as code,
  24 as diagnosis_rank,
  diagnosis_poa_24 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
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
  diagnosis_code_type as code_type,
  diagnosis_code_25 as code,
  25 as diagnosis_rank,
  diagnosis_poa_25 as present_on_admit_code,
  data_source
from {{ ref('input_layer__medical_claim') }} 
where diagnosis_code_25 is not null

)



select distinct
  eg.encounter_id,
  unpivot_cte.claim_id,
  unpivot_cte.patient_id,
  unpivot_cte.condition_date as condition_date,
  unpivot_cte.condition_type as condition_type,
  unpivot_cte.code_type as code_type,
  unpivot_cte.code as code,
  icd.short_description as description,
  unpivot_cte.diagnosis_rank as diagnosis_rank,
  unpivot_cte.present_on_admit_code as present_on_admit_code,
  poa.present_on_admit_description as present_on_admit_description,
  unpivot_cte.data_source as data_source
from unpivot_cte
    left join {{ ref('claims_preprocessing__encounter_data_for_medical_claims')}} as eg
        on  unpivot_cte.claim_id = eg.claim_id
        and unpivot_cte.patient_id = eg.patient_id
--         and unpivot_cte.data_source = eg.data_source
    left join {{ ref('terminology__icd_10_cm') }} icd
        on unpivot_cte.code = icd.icd_10_cm
    left join {{ ref('terminology__present_on_admission') }} as poa
        on unpivot_cte.present_on_admit_code = poa.present_on_admit_code
