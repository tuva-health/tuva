{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False)) | as_bool
   )
}}

with input_medical as (
  select 
    'medical_claim' as table_name,
    count(distinct patient_id) as patient_count,
    count(distinct claim_id) as claim_count,
    count(*) as record_count,
    sum(paid_amount) as paid_amount,
    sum(allowed_amount) as allowed_amount
  from {{ ref('medical_claim') }}
)
,input_pharmacy as (
  select 
    'pharmacy_claim' as table_name,
    count(distinct patient_id) as patient_count,
    count(distinct claim_id) as claim_count,
    count(*) as record_count,
    sum(paid_amount) as paid_amount,
    sum(allowed_amount) as allowed_amount
  from {{ ref('pharmacy_claim') }}
)
,input_eligibility as (
  select
    'eligibility' as table_name,
    count(distinct patient_id) as patient_count
  from {{ ref('eligibility') }}
)

-- Core layer CTEs
,core_medical as (
  select 
    'medical_claim' as table_name,
    count(distinct patient_id) as patient_count,
    count(distinct claim_id) as claim_count,
    count(*) as record_count,
    sum(paid_amount) as paid_amount,
    sum(allowed_amount) as allowed_amount
  from {{ ref('core__medical_claim') }}
)
,core_pharmacy as (
  select 
    'pharmacy_claim' as table_name,
    count(distinct patient_id) as patient_count,
    count(distinct claim_id) as claim_count,
    count(*) as record_count,
    sum(paid_amount) as paid_amount,
    sum(allowed_amount) as allowed_amount
  from {{ ref('core__pharmacy_claim') }}
)
,core_eligibility as (
  select
    'eligibility' as table_name,
    count(distinct patient_id) as patient_count
  from {{ ref('core__member_months') }}
)

-- Combining both input and core layers
select 
  input.table_name,
  'Total Unique Patients' as metric,
  input.patient_count as input_layer_value,
  core.patient_count as core_value
from input_medical input
join core_medical core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Unique Claims' as metric,
  input.claim_count as input_layer_value,
  core.claim_count as core_value
from input_medical input
join core_medical core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Records' as metric,
  input.record_count as input_layer_value,
  core.record_count as core_value
from input_medical input
join core_medical core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Paid Amount' as metric,
  input.paid_amount as input_layer_value,
  core.paid_amount as core_value
from input_medical input
join core_medical core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Allowed Amount' as metric,
  input.allowed_amount as input_layer_value,
  core.allowed_amount as core_value
from input_medical input
join core_medical core
  on input.table_name = core.table_name

union all

-- Pharmacy layer
select 
  input.table_name,
  'Total Unique Patients' as metric,
  input.patient_count as input_layer_value,
  core.patient_count as core_value
from input_pharmacy input
join core_pharmacy core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Unique Claims' as metric,
  input.claim_count as input_layer_value,
  core.claim_count as core_value
from input_pharmacy input
join core_pharmacy core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Records' as metric,
  input.record_count as input_layer_value,
  core.record_count as core_value
from input_pharmacy input
join core_pharmacy core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Paid Amount' as metric,
  input.paid_amount as input_layer_value,
  core.paid_amount as core_value
from input_pharmacy input
join core_pharmacy core
  on input.table_name = core.table_name

union all

select 
  input.table_name,
  'Total Allowed Amount' as metric,
  input.allowed_amount as input_layer_value,
  core.allowed_amount as core_value
from input_pharmacy input
join core_pharmacy core
  on input.table_name = core.table_name

union all

-- Eligibility layer
select 
  input.table_name,
  'Total Unique Patients' as metric,
  input.patient_count as input_layer_value,
  core.patient_count as core_value
from input_eligibility input
join core_eligibility core
  on input.table_name = core.table_name