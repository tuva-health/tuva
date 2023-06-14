{{ config(
     enabled = var('pmpm_enabled',var('tuva_marts_enabled',True))
   )
}}

with claims_with_service_categories as (
select 
  a.patient_id
, b.service_category_1
, b.service_category_2
, coalesce(a.claim_start_date,a.claim_end_date) as claim_date
, a.paid_amount
, a.allowed_amount
from {{ ref('pmpm__stg_medical_claim') }} a
inner join {{ ref('pmpm__stg_service_category_grouper') }} b
  on a.claim_id = b.claim_id
  and a.claim_line_number = b.claim_line_number
)

, medical_claims_year_month as (
select 
  patient_id
, service_category_1
, service_category_2
, cast({{ date_part("year", "claim_date" ) }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month", "claim_date" ) }} as {{ dbt.type_string() }} ),2,'0') AS year_month
, paid_amount
, allowed_amount
from claims_with_service_categories
)

, rx_claims as (
select 
  patient_id
, 'Pharmacy' as service_category_1
, cast(null as {{ dbt.type_string() }}) as service_category_2
, dispensing_date as claim_date
, paid_amount
, allowed_amount
from {{ ref('pmpm__stg_pharmacy_claim') }} 
)

, rx_claims_year_month as (
select 
  patient_id
, service_category_1
, service_category_2
, cast({{ date_part("year", "claim_date" ) }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month", "claim_date" ) }} as {{ dbt.type_string() }} ),2,'0') AS year_month
, paid_amount
, allowed_amount
from rx_claims
)

, combine_medical_and_rx as (
select *
from medical_claims_year_month

union all

select *
from rx_claims_year_month
)

select
  patient_id
, year_month
, service_category_1
, service_category_2
, sum(paid_amount) as total_paid
, sum(allowed_amount) as total_allowed
, '{{ var('last_update')}}' as last_update
from combine_medical_and_rx
group by 1,2,3,4