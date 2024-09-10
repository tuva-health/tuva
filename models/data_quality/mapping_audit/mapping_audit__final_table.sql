{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}




with mc_claim_id as (
select
'medical_claim' as table_name,
'claim_id' as field,
(select count(*) from {{ ref('medical_claim') }} where claim_id is null) as numeric_count
),

mc_claim_line_number as (
select
'medical_claim' as table_name,
'claim_line_number' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where claim_line_number_unique = 'No')
 as numeric_count
),

mc_claim_type as (
select
'medical_claim' as table_name,
'claim_type' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where claim_type_always_populated = 'No' or
       claim_type_unique = 'No' or
       claim_type_valid_values = 'No')
as numeric_count
),

mc_member_id as (
select
'medical_claim' as table_name,
'member_id' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where member_id_always_populated = 'No' or
       member_id_unique = 'No')
as numeric_count
),

mc_plan as (
select
'medical_claim' as table_name,
'plan' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where plan_always_populated = 'No' or
       plan_unique = 'No')
as numeric_count
),

mc_claim_start_date as (
select
'medical_claim' as table_name,
'claim_start_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 -- where claim_start_date_always_populated = 'No' or
 --       claim_start_date_unique = 'No')
where claim_start_date_unique = 'No')
as numeric_count
),

mc_claim_end_date as (
select
'medical_claim' as table_name,
'claim_end_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 -- where claim_end_date_always_populated = 'No' or
 --       claim_end_date_unique = 'No')
where claim_end_date_unique = 'No') 
as numeric_count
),

mc_claim_line_start_date as (
select
'medical_claim' as table_name,
'claim_line_start_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where claim_line_start_date_always_populated = 'No')
as numeric_count
),

mc_claim_line_end_date as (
select
'medical_claim' as table_name,
'claim_line_end_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where claim_line_end_date_always_populated = 'No')
as numeric_count
),

mc_admission_date as (
select
'medical_claim' as table_name,
'admission_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where admission_date_unique = 'No')
as numeric_count
),

mc_discharge_date as (
select
'medical_claim' as table_name,
'discharge_date' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where discharge_date_unique = 'No')
as numeric_count
),

mc_discharge_disposition_code as (
select
'medical_claim' as table_name,
'discharge_disposition_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where discharge_disposition_code_correct_length = 'No' or
       discharge_disposition_code_unique = 'No')
as numeric_count
),

mc_place_of_service_code as (
select
'medical_claim' as table_name,
'place_of_service_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where place_of_service_code_correct_length = 'No')
as numeric_count
),

mc_bill_type_code as (
select
'medical_claim' as table_name,
'bill_type_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where bill_type_code_correct_length = 'No' or
       bill_type_code_unique = 'No')
as numeric_count
),

mc_ms_drg_code as (
select
'medical_claim' as table_name,
'ms_drg_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where ms_drg_code_correct_length = 'No' or
       ms_drg_code_unique = 'No')
as numeric_count
),

mc_apr_drg_code as (
select
'medical_claim' as table_name,
'apr_drg_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where apr_drg_code_correct_length = 'No' or
       apr_drg_code_unique = 'No')
as numeric_count
),

mc_revenue_center_code as (
select
'medical_claim' as table_name,
'revenue_center_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where revenue_center_code_correct_length = 'No')
as numeric_count
),

mc_diagnosis_code_type as (
select
'medical_claim' as table_name,
'diagnosis_code_type' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where diagnosis_code_type_needed = 'Yes' and
 (diagnosis_code_type_valid = 'No' or diagnosis_code_type_unique = 'No')
)
as numeric_count
),

mc_diagnosis_code as (
select
'medical_claim' as table_name,
'diagnosis_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where diagnosis_code_unique = 'No'
)
as numeric_count
),

mc_procedure_code_type as (
select
'medical_claim' as table_name,
'procedure_code_type' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where procedure_code_type_needed = 'Yes' and
 (procedure_code_type_valid = 'No' or procedure_code_type_unique = 'No')
)
as numeric_count
),

mc_procedure_code as (
select
'medical_claim' as table_name,
'procedure_code' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where procedure_code_unique = 'No'
)
as numeric_count
),

mc_data_source as (
select
'medical_claim' as table_name,
'data_source' as field,
(select count(*)
 from {{ ref('mapping_audit__mc_claim_grain_final') }}
 where data_source_always_populated = 'No' or
       data_source_unique = 'No')
as numeric_count
),

pc_claim_id as (
select
'pharmacy_claim' as table_name,
'claim_id' as field,
(select count(*) from {{ ref('pharmacy_claim') }} where claim_id is null) as numeric_count
),

pc_claim_line_number as (
select
'pharmacy_claim' as table_name,
'claim_line_number' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where claim_line_number_unique = 'No')
 as numeric_count
),

pc_member_id as (
select
'pharmacy_claim' as table_name,
'member_id' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where member_id_always_populated = 'No' or
       member_id_unique = 'No')
as numeric_count
),

pc_plan as (
select
'pharmacy_claim' as table_name,
'plan' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where plan_always_populated = 'No' or
       plan_unique = 'No')
as numeric_count
),

pc_ndc_code as (
select
'pharmacy_claim' as table_name,
'ndc_code' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where ndc_code_correct_length = 'No' or
       ndc_code_always_populated = 'No')
as numeric_count
),

pc_quantity as (
select
'pharmacy_claim' as table_name,
'quantity' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where quantity_is_positive_integer = 'No')
as numeric_count
),

pc_days_supply as (
select
'pharmacy_claim' as table_name,
'days_supply' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where days_supply_is_positive_integer = 'No')
as numeric_count
),

pc_refills as (
select
'pharmacy_claim' as table_name,
'refills' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where refills_is_positive_integer = 'No')
as numeric_count
),

pc_in_network_flag as (
select
'pharmacy_claim' as table_name,
'in_network_flag' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where in_network_flag_unique = 'No' or
       in_network_flag_valid_values = 'No')
as numeric_count
),

pc_data_source as (
select
'medical_claim' as table_name,
'data_source' as field,
(select count(*)
 from {{ ref('mapping_audit__pc_claim_grain_final') }}
 where data_source_always_populated = 'No' or
       data_source_unique = 'No')
as numeric_count
)




select *
from mc_claim_id
union all
select *
from mc_claim_line_number
union all
select *
from mc_claim_type
union all
select *
from mc_member_id
union all
select *
from mc_plan
union all
select *
from mc_claim_start_date
union all
select *
from mc_claim_end_date
union all
select *
from mc_admission_date
union all
select *
from mc_discharge_date
union all
select *
from mc_discharge_disposition_code
union all
select *
from mc_place_of_service_code
union all
select *
from mc_bill_type_code
union all
select *
from mc_ms_drg_code
union all
select *
from mc_apr_drg_code
union all
select *
from mc_revenue_center_code
union all
select *
from mc_diagnosis_code_type
union all
select *
from mc_diagnosis_code
union all
select *
from mc_procedure_code_type
union all
select *
from mc_procedure_code
union all
select *
from mc_data_source
union all
select *
from pc_claim_line_number
union all
select *
from pc_member_id
union all
select *
from pc_plan
union all
select *
from pc_ndc_code
union all
select *
from pc_quantity
union all
select *
from pc_days_supply
union all
select *
from pc_refills
union all
select *
from pc_in_network_flag
union all
select *
from pc_data_source
