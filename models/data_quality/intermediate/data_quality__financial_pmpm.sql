{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select distinct cast(year_month_int as {{ dbt.type_string() }} ) as year_month
,year as year_number
from {{ ref('reference_data__calendar') }}
)

select  count(*) as member_months
,sum(total_paid) / cast(count(*) as {{ dbt.type_numeric() }}) as total_pmpm
,c.year_number as year_number
from {{ ref('financial_pmpm__pmpm_prep') }} p 
left join cte c on p.year_month = c.year_month 
group by c.year_number