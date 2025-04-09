{{ config(
     enabled = var('claims_enabled',False)
   )
}}

with unioned_data as (
select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_allowed_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_charge_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_claim_id') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_claim_line_end_date') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_claim_line_number') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_claim_line_start_date') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_claim_type') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_coinsurance_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_copayment_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_data_source') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_deductible_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_diagnosis_code_type') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_hcpcs_code') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_member_id') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_paid_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_paid_date') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_person_id') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_payer') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_plan') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_total_cost_amount') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_address') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_birth_date') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_city') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_data_source') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_death_date') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_death_flag') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_dual_status_code') }}

union all

select
    cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_end_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_first_name') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_gender') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_last_name') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_medicare_status_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_member_id') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_original_reason_entitlement_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_person_id') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_payer_type') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_payer') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_phone') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_plan') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_race') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_start_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_state') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__eligibility_zip_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_drg_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_admission_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_admit_source_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_admit_type_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_bill_type_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_billing_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_claim_end_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_claim_start_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_diagnosis_code_1') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_diagnosis_code_2') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_diagnosis_code_3') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_discharge_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_discharge_disposition_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_facility_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_present_on_admission_1') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_present_on_admission_2') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_present_on_admission_3') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_code_1') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_code_2') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_code_3') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_date_1') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_date_2') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_procedure_date_3') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_rendering_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_revenue_center_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__institutional_service_unit_quantity') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_allowed_amount') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_claim_id') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_claim_line_number') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_coinsurance_amount') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_copayment_amount') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_data_source') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_days_supply') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_deductible_amount') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_dispensing_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_dispensing_provider_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_member_id') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_ndc_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_paid_amount') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_paid_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_person_id') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_payer') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_plan') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_prescribing_provider_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_quantity') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__pharmacy_refills') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_billing_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_facility_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_place_of_service_code') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_rendering_npi') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_claim_end_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_claim_start_date') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_diagnosis_code_1') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_diagnosis_code_2') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__professional_diagnosis_code_3') }}

union all

select     cast(data_source as {{ dbt.type_string() }}) as data_source
	, cast(source_date as {{ dbt.type_string() }}) as source_date
	, cast(table_name as {{ dbt.type_string() }}) as table_name
	, cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
	, cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
	, cast(claim_type as {{ dbt.type_string() }}) as claim_type
	, cast(field_name as {{ dbt.type_string() }}) as field_name
	, cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
	, cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
	, cast(field_value as {{ dbt.type_string() }}) as field_value
	, cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
	from {{ ref('data_quality__claim_drg_code_type') }}

)


select
    cast(data_source as {{ dbt.type_string() }}) as data_source
  , cast(source_date as {{ dbt.type_string() }}) as source_date
  , cast(table_name as {{ dbt.type_string() }}) as table_name
  , cast(drill_down_key as {{ dbt.type_string() }}) as drill_down_key
  , cast(drill_down_value as {{ dbt.type_string() }}) as drill_down_value
  , cast(claim_type as {{ dbt.type_string() }}) as claim_type
  , cast(field_name as {{ dbt.type_string() }}) as field_name
  , cast(bucket_name as {{ dbt.type_string() }}) as bucket_name
  , cast(invalid_reason as {{ dbt.type_string() }}) as invalid_reason
  , cast(field_value as {{ dbt.type_string() }}) as field_value
  , cast(tuva_last_run as {{ dbt.type_string() }}) as tuva_last_run
  , dense_rank() over (
        order by data_source
               , table_name
               , claim_type
               , field_name
    ) as summary_sk
from unioned_data
