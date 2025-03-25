{{ config(
    enabled = var('pqi_enabled', var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
) }}

with

date_int as (
    select distinct
        replace(year_month, '-', '') as yyyymm
      , first_day_of_month
    from {{ ref('reference_data__calendar') }} as c
)

select
    data_source
  , person_id
  , first_day_of_month
  , d.yyyymm as year_month

from
    {{ ref('core__member_months') }} as mm
inner join date_int as d on mm.year_month = d.yyyymm
