
-- This dbt model lists all acute inpatient institutional claims together
-- with relevant fields and data quality flags for those claims. In this
-- model we define the logic for what constitutes an acute inpatient
-- institutional claim by pulling all claim_ids from the
-- 'aip_venn_diagram' model that meet the logic we have chosen (e.g. all
-- claims from the 'aip_venn_diagram' model with flag rb_drg_bill = 1).

-- By default this model is written so that an acute inpatient
-- institutional claim is one that meets the 3 requirements (DRG
-- requirement, bill type requirement, and room & board requirement),
-- i.e. a claim that has rb_drg_bill = 1 in the 'aip_venn_diagram'
-- model. This logic can be modified inside this model if, for example,
-- we are missing a key field in the data source.

-- This dbt model has these columns:
--     claim_id
--     patient_id
--     claim_start_date
--     claim_end_date
--     admission_date
--     discharge_date
--     admit_source_code
--     admit_type_code
--     discharge_disposition_code
--     facility_npi
--     claim_type
--     ms_drg_code
--     apr_drg_code
--     rendering_npi,
--     diagnosis_code_1
--     start_date
--     end_date
--     date_used_as_start_date
--     date_used_as_end_date
--     patient_id_missing,
--     claim_start_date_missing
--     claim_end_date_missing
--     claim_start_date_after_claim_end_date
--     admission_date_missing
--     discharge_date_missing
--     admission_date_after_discharge_date
--     admit_type_code_missing,
--     admit_source_code_missing,
--     discharge_disposition_code_missing
--     facility_npi_missing
--     claim_type_missing
--     claim_type_not_institutional
--     ms_drg_code_missing
--     apr_drg_code_missing
--     diagnosis_code_1_missing
--     rendering_npi_missing






-- Relevant fields for each claim_id (at the claim_id grain):
with medical_claims_relevant_fields as (
select
  claim_id,
  max(patient_id) as patient_id,
  max(claim_start_date) as claim_start_date,
  max(claim_end_date) as claim_end_date,
  max(admission_date) as admission_date,
  max(discharge_date) as discharge_date,
  max(admit_source_code) as admit_source_code,
  max(admit_type_code) as admit_type_code,
  max(discharge_disposition_code) as discharge_disposition_code,
  max(facility_npi) as facility_npi,
  max(claim_type) as claim_type,
  max(ms_drg_code) as ms_drg_code,
  max(apr_drg_code) as apr_drg_code,
  max(rendering_npi) as rendering_npi

from {{ ref('core__medical_claim') }} aa

group by claim_id
),


-- Grab only acute inpatient institutional claims
-- by left joining with relevant rows of 'aip_venn_diagram'.
aip_inst_claims as (
select
  aa.claim_id,
  bb.patient_id,
  bb.claim_start_date,
  bb.claim_end_date,
  bb.admission_date,
  bb.discharge_date,
  bb.admit_source_code,
  bb.admit_type_code,
  bb.discharge_disposition_code,
  bb.facility_npi,
  bb.claim_type,
  bb.ms_drg_code,
  bb.apr_drg_code,
  bb.rendering_npi

from {{ ref('aip_venn_diagram') }} aa

left join medical_claims_relevant_fields bb 
on aa.claim_id = bb.claim_id

-- ********************************************************************
-- Here we define the logic for what part of the Venn Diagram
-- determines which claims are tagged as institutional acute IP claims:
-- ********************************************************************
where aa.rb_drg_bill = 1
),



-- Grab primary diagnosis code from the 'core.condition' table:
add_primary_dx as (
select
  aa.claim_id,
  aa.patient_id,
  aa.claim_start_date,
  aa.claim_end_date,
  aa.admission_date,
  aa.discharge_date,
  aa.admit_source_code,
  aa.admit_type_code,
  aa.discharge_disposition_code,
  aa.facility_npi,
  aa.claim_type,
  aa.ms_drg_code,
  aa.apr_drg_code,
  aa.rendering_npi,
  bb.normalized_code as diagnosis_code_1

from aip_inst_claims aa 
left join {{ ref('core__condition') }} bb
on aa.claim_id = bb.claim_id
where bb.condition_rank = 1
),


-- Define start and end dates for each claim:
add_start_and_end_dates as (
select
  claim_id,
  patient_id,
  claim_start_date,
  claim_end_date,
  admission_date,
  discharge_date,
  admit_source_code,
  admit_type_code,
  discharge_disposition_code,
  facility_npi,
  claim_type,
  ms_drg_code,
  apr_drg_code,
  rendering_npi,  
  diagnosis_code_1,

  coalesce(admission_date,claim_start_date) as start_date,
  coalesce(discharge_date,claim_end_date) as end_date,
  
  case
    when admission_date is not null then 'admission_date'
    when claim_start_date is not null then 'claim_start_date'
    else null
  end as date_used_as_start_date,
  
  case
    when discharge_date is not null then 'discharge_date'
    when claim_end_date is not null then 'claim_end_date'
    else null
  end as date_used_as_end_date

from add_primary_dx
),



-- Add data quality flags:
add_dq_flags as (
select
  claim_id,
  patient_id,
  claim_start_date,
  claim_end_date,
  admission_date,
  discharge_date,
  admit_source_code,
  admit_type_code,
  discharge_disposition_code,
  facility_npi,
  claim_type,
  ms_drg_code,
  apr_drg_code,
  rendering_npi,  
  diagnosis_code_1,
  start_date,
  end_date,
  date_used_as_start_date,
  date_used_as_end_date,
  
-- Here we add the DQ flags:

-- patient_id_missing:  
  case
    when patient_id is null then 1
    else 0
  end as patient_id_missing,  
-- claim_start_date_missing:  
  case
    when claim_start_date is null then 1
    else 0
  end as claim_start_date_missing,
-- claim_end_date_missing:  
  case
    when claim_end_date is null then 1
    else 0
  end as claim_end_date_missing,
-- claim_start_date_after_claim_end_date:
  case
    when claim_start_date > claim_end_date then 1
    else 0
  end as claim_start_date_after_claim_end_date,
-- admission_date_missing:  
  case
    when admission_date is null then 1
    else 0
  end as admission_date_missing,
-- discharge_date_missing:  
  case
    when discharge_date is null then 1
    else 0
  end as discharge_date_missing,
-- admission_date_after_discharge_date:
  case
    when admission_date > discharge_date then 1
    else 0
  end as admission_date_after_discharge_date,
-- admit_type_code_missing:  
  case
    when admit_type_code is null then 1
    else 0
  end as admit_type_code_missing,  
-- admit_source_code_missing:  
  case
    when admit_source_code is null then 1
    else 0
  end as admit_source_code_missing,  
-- discharge_disposition_code_missing:  
  case
    when discharge_disposition_code is null then 1
    else 0
  end as discharge_disposition_code_missing,
-- facility_npi_missing:  
  case
    when facility_npi is null then 1
    else 0
  end as facility_npi_missing,
-- claim_type_missing:  
  case
    when claim_type is null then 1
    else 0
  end as claim_type_missing,
-- claim_type_not_institutional:
  case
    when claim_type <> 'institutional' then 1
    else 0
  end as claim_type_not_institutional,
-- ms_drg_code_missing:  
  case
    when ms_drg_code is null then 1
    else 0
  end as ms_drg_code_missing,
-- apr_drg_code_missing:  
  case
    when apr_drg_code is null then 1
    else 0
  end as apr_drg_code_missing,
-- diagnosis_code_1_missing:  
  case
    when diagnosis_code_1 is null then 1
    else 0
  end as diagnosis_code_1_missing,
-- rendering_npi_missing:  
  case
    when rendering_npi is null then 1
    else 0
  end as rendering_npi_missing

from add_start_and_end_dates

)




select *
from add_dq_flags

