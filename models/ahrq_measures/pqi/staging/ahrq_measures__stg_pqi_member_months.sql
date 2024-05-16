{{ config(
     enabled = var('pqi_enabled',var('claims_enabled',var('tuva_marts_enabled',False))) | as_bool
   )
}}

with date_int as (
    select distinct
        replace(year_month,'-','') yyyymm
      , first_day_of_month
    from {{ ref('terminology__calendar') }} as c
)

select 
    data_source
  , patient_id
  , first_day_of_month
  , d.yyyymm as year_month

from 
    {{ ref('financial_pmpm__member_months') }} mm
inner join date_int d on mm.year_month = d.yyyymm 
