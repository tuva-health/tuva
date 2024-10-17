{{ config(enabled = var('claims_enabled', var('tuva_marts_enabled', false)) | as_bool) }}

with member_months as (
    select 
           year_month
         , count(1) as member_months
    from {{ ref('core__member_months') }}
    group by year_month
)

, service_categories as (
    select 
           to_char(claim_start_date, 'YYYYMM') as year_month
         , service_category_1
         , service_category_2
         , sum(paid_amount) as total_paid
    from {{ ref('core__medical_claim') }}
    group by 
             to_char(claim_start_date, 'YYYYMM')
           , service_category_1
           , service_category_2
)

select 
       scat.year_month
     , scat.service_category_1
     , scat.service_category_2
     , scat.total_paid
     , scat.total_paid / mm.member_months as pmpm
from service_categories as scat
inner join member_months as mm on scat.year_month = mm.year_month
order by 
         scat.year_month
       , scat.service_category_1
