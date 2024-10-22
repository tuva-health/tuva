{{ config(
     enabled = var('cms_hcc_enabled',var('claims_enabled',var('tuva_marts_enabled',false))) | as_bool
   )
}}

with collection_date_range as (

    select 
        date_trunc('year', min(claim_end_date)) as start_date
        , max(claim_end_date) as end_date
    from {{ ref('medical_claim') }}

)

select distinct
    date_trunc(year, calendar.last_day_of_month) as collection_start_date
    , calendar.last_day_of_month as collection_end_date
    , calendar.year as collection_year
    , calendar.year + 1 as payment_year
from {{ ref('reference_data__calendar') }} as calendar
inner join collection_date_range
    on calendar.first_day_of_month 
        between collection_date_range.start_date and collection_date_range.end_date
