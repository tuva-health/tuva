{{ config(
     enabled = var('claims_preprocessing_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

-- *************************************************
-- This dbt model summarizes data quality issues
-- that could affect claims preprocessing.
-- The table returns 2 fields:
--        field
--        total_count
--
-- If we order the results by 'field', the first
-- 3 rows give us a high level summary:
--
--       01 Total acute inpatient institutional claims:
--             This is the total number of acute inpatient
--             institutional claims
--       02 Acute inpatient institutional claims with insights:
--             This is the number of acute inpatient institutional
--             claims for which something is not ideal (e.g. they
--             are missing a claim_start_date) but it's not something
--             that prevents the claim from being used in the
--             encounter grouper.

--       03 Acute inpatient institutional claims with problems:
--             This is the number of acute inpatient institutional
--             claims that have a data quality problem that
--             prevents them from being used in the encounter grouper.

-- The rest of the rows in the table give us counts
-- of the number of acute inpatient institutional claims
-- that have specific data quality problems.
-- *************************************************




with total_acute_inpatient__institutional_claims as (
select
  '01 Total acute inpatient institutional claims' as field,
  count(*) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


total_claims_with_insights as (
select
  '02 Acute inpatient institutional claims with insights' as field,
  count(*) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
where dq_insight = 1
),


total_claims_with_problems as (
select
  '03 Acute inpatient institutional claims with problems' as field,
  count(*) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
where dq_problem = 1
),


patient_id_not_unique as (
select
  '04 patient_id_not_unique' as field,
  sum(patient_id_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


patient_id_missing as (
select
  '05 patient_id_missing' as field,
  sum(patient_id_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_start_date_not_unique as (
select
  '06 claim_start_date_not_unique' as field,
  sum(claim_start_date_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_start_date_missing as (
select
  '07 claim_start_date_missing' as field,
  sum(claim_start_date_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_end_date_not_unique as (
select
  '08 claim_end_date_not_unique' as field,
  sum(claim_end_date_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_end_date_missing as (
select
  '09 claim_end_date_missing' as field,
  sum(claim_end_date_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_start_date_after_claim_end_date as (
select
  '10 claim_start_date_after_claim_end_date' as field,
  sum(claim_start_date_after_claim_end_date) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admission_date_not_unique as (
select
  '11 admission_date_not_unique' as field,
  sum(admission_date_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admission_date_missing as (
select
  '12 admission_date_missing' as field,
  sum(admission_date_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


discharge_date_not_unique as (
select
  '13 discharge_date_not_unique' as field,
  sum(discharge_date_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


discharge_date_missing as (
select
  '14 discharge_date_missing' as field,
  sum(discharge_date_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admission_date_after_discharge_date as (
select
  '15 admission_date_after_discharge_date' as field,
  sum(admission_date_after_discharge_date) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admit_type_code_not_unique as (
select
  '16 admit_type_code_not_unique' as field,
  sum(admit_type_code_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admit_type_code_missing as (
select
  '17 admit_type_code_missing' as field,
  sum(admit_type_code_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admit_source_code_not_unique as (
select
  '18 admit_source_code_not_unique' as field,
  sum(admit_source_code_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


admit_source_code_missing as (
select
  '19 admit_source_code_missing' as field,
  sum(admit_source_code_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


discharge_disposition_code_not_unique as (
select
  '20 discharge_disposition_code_not_unique' as field,
  sum(discharge_disposition_code_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


discharge_disposition_code_missing as (
select
  '21 discharge_disposition_code_missing' as field,
  sum(discharge_disposition_code_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


facility_npi_not_unique as (
select
  '22 facility_npi_not_unique' as field,
  sum(facility_npi_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


facility_npi_missing as (
select
  '23 facility_npi_missing' as field,
  sum(facility_npi_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_type_not_unique as (
select
  '24 claim_type_not_unique' as field,
  sum(claim_type_not_unique) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_type_missing as (
select
  '25 claim_type_missing' as field,
  sum(claim_type_missing) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


claim_type_not_institutional as (
select
  '26 claim_type_not_institutional' as field,
  sum(claim_type_not_institutional) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


start_date_not_determined as (
select
  '27 start_date_not_determined' as field,
  sum(start_date_not_determined) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


end_date_not_determined as (
select
  '28 end_date_not_determined' as field,
  sum(end_date_not_determined) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),


start_date_after_end_date as (
select
  '29 start_date_after_end_date' as field,
  sum(start_date_after_end_date) as total_count
from {{ ref('acute_inpatient__institutional_claims') }}
),



union_cte as (
select *
from total_acute_inpatient__institutional_claims

union all

select *
from total_claims_with_insights

union all

select *
from total_claims_with_problems

union all

select *
from patient_id_not_unique

union all

select *
from patient_id_missing

union all

select *
from claim_start_date_not_unique

union all

select *
from claim_start_date_missing

union all

select *
from claim_end_date_not_unique

union all

select *
from claim_end_date_missing

union all

select *
from claim_start_date_after_claim_end_date

union all

select *
from admission_date_not_unique

union all

select *
from admission_date_missing

union all

select *
from discharge_date_not_unique

union all

select *
from discharge_date_missing

union all

select *
from admission_date_after_discharge_date

union all

select *
from admit_type_code_not_unique

union all

select *
from admit_type_code_missing

union all

select *
from admit_source_code_not_unique

union all

select *
from admit_source_code_missing

union all

select *
from discharge_disposition_code_not_unique

union all

select *
from discharge_disposition_code_missing

union all

select *
from facility_npi_not_unique

union all

select *
from facility_npi_missing

union all

select *
from claim_type_not_unique

union all

select *
from claim_type_missing

union all

select *
from claim_type_not_institutional

union all

select *
from start_date_not_determined

union all

select *
from end_date_not_determined

union all

select *
from start_date_after_end_date
)



select *, '{{ var('tuva_last_run')}}' as tuva_last_run
from union_cte
