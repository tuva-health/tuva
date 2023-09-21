{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with trend_service_category_1 as (
    select 
        year(claim_end_date) || month(claim_end_date) as year_month
        , service_category_1
        , count(distinct claim_id) as distinct_claim_count
        , lag(count(distinct claim_id),1) over (partition by service_category_1 order by year(claim_end_date) || month(claim_end_date)) as previous_distinct_claim_count
    from {{ ref('core__medical_claim') }}
    group by 
        year(claim_end_date)
        , month(claim_end_date)
        , service_category_1
)
, trend_service_category_2 as (
    select 
        year(claim_end_date) || month(claim_end_date) as year_month
        , service_category_2
        , count(distinct claim_id) as distinct_claim_count
        , lag(count(distinct claim_id),1) over (partition by service_category_2 order by year(claim_end_date) || month(claim_end_date)) as previous_distinct_claim_count
    from {{ ref('core__medical_claim') }}
    group by 
        year(claim_end_date)
        , month(claim_end_date)
        , service_category_2
)

select 
    year_month
    , service_category_1
    , distinct_claim_count
    , distinct_claim_count-previous_distinct_claim_count as distinct_claim_count_change
    , ((distinct_claim_count-previous_distinct_claim_count) / distinct_claim_count) * 100 as percent_change
from trend_service_category_1

 union all

 select 
    year_month
    , service_category_2
    , distinct_claim_count
    , distinct_claim_count-previous_distinct_claim_count as distinct_claim_count_change
    , ((distinct_claim_count-previous_distinct_claim_count) / distinct_claim_count) * 100 as percent_change
from trend_service_category_2