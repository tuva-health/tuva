{{ config(
     enabled = var('core_enabled',var('tuva_marts_enabled',True))
   )
}}

-- *************************************************
-- This dbt model creates the encounter table in core.
-- *************************************************




with table_without_descriptions as (
select
  aa.encounter_id,
  max(aa.patient_id) as patient_id,
  max(aa.encounter_type) as encounter_type,
  max(eg.encounter_start_date) as encounter_start_date,
  max(eg.encounter_end_date) as encounter_end_date,
   max(eg.encounter_admit_source_code) as encounter_admit_source_code,
   max(eg.encounter_admit_type_code) as encounter_admit_type_code,
   max(eg.encounter_discharge_disposition_code) as encounter_discharge_disposition_code,
  max(aa.rendering_npi) as rendering_npi,
  max(aa.billing_npi) as billing_npi,
  max(aa.facility_npi) as facility_npi,
  max(aa.ms_drg_code) as ms_drg_code,
  max(aa.apr_drg_code) as apr_drg_code,
  max(aa.paid_date) as paid_date,
  sum(aa.paid_amount) as paid_amount,
  sum(aa.allowed_amount) as allowed_amount,
  sum(aa.charge_amount) as charge_amount,
  sum(aa.total_cost_amount) as total_cost_amount,
  max(aa.data_source) as data_source
from {{ ref('core__medical_claim') }} aa
left join {{ ref('claims_preprocessing__encounter_grouper')}} as eg
    on  aa.claim_id = eg.claim_id
    and aa.claim_line_number = eg.claim_line_number
    and aa.patient_id = eg.patient_id
--         and aa.data_source = eg.data_source
where aa.encounter_id is not null
group by aa.encounter_id
),


unique_apr_drg_codes as (
select distinct apr_drg_code, apr_drg_description
from {{ ref('terminology__apr_drg') }} 
),


add_descriptions as (
select
  aa.encounter_id,
  aa.patient_id as patient_id,
  aa.encounter_type as encounter_type,
  aa.encounter_start_date as encounter_start_date,
  aa.encounter_end_date as encounter_end_date,
  aa.encounter_admit_source_code as admit_source_code,
  bb.admit_source_description as admit_source_description,
  aa.encounter_admit_type_code as admit_type_code,
  cc.admit_type_description admit_type_description,
  aa.encounter_discharge_disposition_code as discharge_disposition_code,
  dd.discharge_disposition_description as
     discharge_disposition_description,
  aa.rendering_npi as rendering_npi,
  aa.billing_npi as billing_npi,
  aa.facility_npi as facility_npi,
  aa.ms_drg_code as ms_drg_code,
  ee.ms_drg_description as ms_drg_description,
  aa.apr_drg_code as apr_drg_code,
  ff.apr_drg_description as apr_drg_description,
  aa.paid_date as paid_date,
  aa.paid_amount as paid_amount,
  aa.allowed_amount as allowed_amount,
  aa.charge_amount as charge_amount,
  aa.total_cost_amount as total_cost_amount,
  aa.data_source as data_source

from table_without_descriptions aa

     left join {{ ref('terminology__admit_source') }} bb
     on aa.encounter_admit_source_code = bb.admit_source_code
     left join {{ ref('terminology__admit_type') }} cc
     on aa.encounter_admit_type_code = cc.admit_type_code
     left join {{ ref('terminology__discharge_disposition') }} dd
     on aa.encounter_discharge_disposition_code = dd.discharge_disposition_code
     left join {{ ref('terminology__ms_drg') }} ee
     on aa.ms_drg_code = ee.ms_drg_code
     left join unique_apr_drg_codes ff
     on aa.apr_drg_code = ff.apr_drg_code
     
)


select *
from add_descriptions
