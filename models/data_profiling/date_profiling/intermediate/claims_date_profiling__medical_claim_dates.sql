{{ config(
     enabled = var('claims_date_profiling_enabled',var('tuva_marts_enabled',True))
   )
}}

with med_claims as (
select
  min(claim_start_date) as claim_start_date
, min(claim_end_date) as claim_end_date
, min(admission_date) as admission_date
, min(discharge_date) as discharge_date
, min(paid_date) as paid_date
, claim_id
from {{ ref('medical_claim') }} 
group by claim_id
)

, transform as (
select
  cast({{ date_part("year","claim_start_date") }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month","claim_start_date") }} as {{ dbt.type_string() }} ),2,'0') as claim_start_date
, cast({{ date_part("year","claim_end_date") }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month","claim_end_date") }} as {{ dbt.type_string() }} ),2,'0') as claim_end_date
, cast({{ date_part("year","admission_date") }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month","admission_date") }} as {{ dbt.type_string() }} ),2,'0') as admission_date
, cast({{ date_part("year","discharge_date") }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month","discharge_date") }} as {{ dbt.type_string() }} ),2,'0') as discharge_date
, cast({{ date_part("year","paid_date") }} as {{ dbt.type_string() }} ) || lpad(cast({{ date_part("month","paid_date") }} as {{ dbt.type_string() }} ),2,'0') as paid_date
, claim_id
from med_claims
)

, pivot_prep as (
select
  cast(claim_start_date as {{ dbt.type_string() }} ) as year_month
, 'claim_start_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  cast(claim_end_date as {{ dbt.type_string() }} ) as year_month
, 'claim_end_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  cast(admission_date as {{ dbt.type_string() }} ) as year_month
, 'admission_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  cast(discharge_date as {{ dbt.type_string() }} ) as year_month
, 'discharge_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2

union all

select
  cast(paid_date as {{ dbt.type_string() }} ) as year_month
, 'paid_date' as date_type
, count(distinct claim_id) as cnt
from transform 
group by 1,2
)

select 
year_month
, {{ dbt_utils.pivot(
        column='date_type'
    , values=['claim_start_date','claim_end_date','admission_date','discharge_date','paid_date']
    , agg='sum'
    , quote_identifiers=false
    ) }}
, '{{ var('last_update')}}' as last_update
from pivot_prep
group by year_month