{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with input_medical as (
  select
      cast('medical_claim' as {{ dbt.type_string() }}) as table_name
    , count(distinct person_id) as patient_count
    , count(distinct claim_id) as claim_count
    , count(*) as record_count
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
  from {{ ref('input_layer__medical_claim') }}
)

, input_pharmacy as (
  select
      cast('pharmacy_claim' as {{ dbt.type_string() }}) as table_name
    , count(distinct person_id) as patient_count
    , count(distinct claim_id) as claim_count
    , count(*) as record_count
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
  from {{ ref('input_layer__pharmacy_claim') }}
)

,input_eligibility as (
select
    cast('eligibility' as {{ dbt.type_string() }}) as table_name
  , count(distinct person_id) as patient_count
  , count(distinct {{ concat_custom([
        'member_id'
      , "'-'"
      , 'enrollment_start_date'
      , "'-'"
      , 'enrollment_end_date'
      , "'-'"
      , 'payer'
      , "'-'"
      , quote_column('plan')
    ]) }}) as span_count
from {{ ref('input_layer__eligibility') }}
)

  , input_member_months as (
  select
      cast('eligibility' as {{ dbt.type_string() }}) as table_name
    , count(*) as member_month_count
  from {{ ref('data_quality__eligibility_dq_stage') }} e
)

-- Core layer CTEs
, core_medical as (
  select
     cast( 'medical_claim' as {{ dbt.type_string() }}) as table_name
    , count(distinct person_id) as patient_count
    , count(distinct claim_id) as claim_count
    , count(*) as record_count
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
  from {{ ref('core__medical_claim') }}
)

, core_pharmacy as (
  select
     cast( 'pharmacy_claim' as {{ dbt.type_string() }}) as table_name
    , count(distinct person_id) as patient_count
    , count(distinct claim_id) as claim_count
    , count(*) as record_count
    , sum(paid_amount) as paid_amount
    , sum(allowed_amount) as allowed_amount
  from {{ ref('core__pharmacy_claim') }}
)

, core_eligibility as (
  select
    cast('eligibility'  as {{ dbt.type_string() }})as table_name
    , count(distinct person_id) as patient_count
    , count(*) as span_count
  from {{ ref('core__eligibility') }}
)

, core_member_months as (
  select
    cast('member_months' as {{ dbt.type_string() }}) as table_name
    , count(*) as member_month_count
  from {{ ref('core__member_months') }}
)

,final as (
-- Combining both input and core layers
select
    input.table_name
  , cast('Total Unique Patients' as {{ dbt.type_string() }}) as metric
  , input.patient_count as input_layer_value
  , core.patient_count as core_value
from input_medical as input
inner join core_medical as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Unique Claims' as {{ dbt.type_string() }}) as metric
  , input.claim_count as input_layer_value
  , core.claim_count as core_value
from input_medical as input
inner join core_medical as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Records' as {{ dbt.type_string() }}) as metric
  , input.record_count as input_layer_value
  , core.record_count as core_value
from input_medical as input
inner join core_medical as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Paid Amount' as {{ dbt.type_string() }}) as metric
  , input.paid_amount as input_layer_value
  , core.paid_amount as core_value
from input_medical as input
inner join core_medical as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Allowed Amount' as {{ dbt.type_string() }}) as metric
  , input.allowed_amount as input_layer_value
  , core.allowed_amount as core_value
from input_medical as input
inner join core_medical as core
  on input.table_name = core.table_name

-- Pharmacy layer
union all

select
    input.table_name
  , cast('Total Unique Patients' as {{ dbt.type_string() }}) as metric
  , input.patient_count as input_layer_value
  , core.patient_count as core_value
from input_pharmacy as input
inner join core_pharmacy as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Unique Claims' as {{ dbt.type_string() }}) as metric
  , input.claim_count as input_layer_value
  , core.claim_count as core_value
from input_pharmacy as input
inner join core_pharmacy as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Records' as {{ dbt.type_string() }}) as metric
  , input.record_count as input_layer_value
  , core.record_count as core_value
from input_pharmacy as input
inner join core_pharmacy as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Paid Amount' as {{ dbt.type_string() }}) as metric
  , input.paid_amount as input_layer_value
  , core.paid_amount as core_value
from input_pharmacy as input
inner join core_pharmacy as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Allowed Amount' as {{ dbt.type_string() }}) as metric
  , input.allowed_amount as input_layer_value
  , core.allowed_amount as core_value
from input_pharmacy as input
inner join core_pharmacy as core
  on input.table_name = core.table_name

-- Eligibility layer
union all

select
    input.table_name
  , cast('Total Unique Patients' as {{ dbt.type_string() }}) as metric
  , input.patient_count as input_layer_value
  , core.patient_count as core_value
from input_eligibility as input
inner join core_eligibility as core
  on input.table_name = core.table_name


union all

select
    input.table_name
  , cast('Total Unique Eligibility Spans' as {{ dbt.type_string() }}) as metric
  , input.span_count as input_layer_value
  , core.span_count as core_value
from input_eligibility as input
inner join core_eligibility as core
  on input.table_name = core.table_name

union all

select
    input.table_name
  , cast('Total Member Months' as {{ dbt.type_string() }}) as metric
  , input.member_month_count as input_layer_value
  , core.member_month_count as core_value
from input_member_months as input
inner join core_member_months as core
  on input.table_name = core.table_name

)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final