with trend_by_claim_type as(
    select
        cast(year(claim_end_date) as varchar) || right('0'||cast(month(claim_end_date) as varchar),2) as year_month
        , claim_type
        , count(distinct claim_id) as distinct_claim_count
    from {{ ref('core__medical_claim') }}
    group by 
        year(claim_end_date)
        , month(claim_end_date)
        , claim_type
)
, trend_with_previous_count as(
 select 
    year_month
    , claim_type
    , distinct_claim_count
    , lag(distinct_claim_count) over (partition by claim_type order by year_month) as previous_claim_count
 from trend_by_claim_type
)

select
    year_month
    , claim_type
    , distinct_claim_count
    , distinct_claim_count - previous_claim_count as distinct_claim_count_change
    , ((distinct_claim_count - previous_claim_count)/distinct_claim_count)*100 as distinct_claim_percent_change
from trend_with_previous_count