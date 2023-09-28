{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with trend_by_service_category_1 as (
    select 
        cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , service_category_1
        , count(distinct claim_id) as distinct_claim_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
        , service_category_1
)
, trend_by_service_category_2 as (
    select 
        cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , service_category_2
        , count(distinct claim_id) as distinct_claim_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
        , service_category_2
)
, previous_service_category_1_claim_count as(
    select
        year_month
        , service_category_1
        , distinct_claim_count
        , lag(distinct_claim_count) over (partition by service_category_1 order by year_month) as previous_distinct_claim_count
    from trend_by_service_category_1
)
, previous_service_category_2_claim_count as(
    select
        year_month
        , service_category_2
        , distinct_claim_count
        , lag(distinct_claim_count) over (partition by service_category_2 order by year_month) as previous_distinct_claim_count
    from trend_by_service_category_2
)
select 
    year_month
    , service_category_1 as service_category
    , distinct_claim_count
    , distinct_claim_count-previous_distinct_claim_count as distinct_claim_count_change
    , case
        when distinct_claim_count <> 0 then ((distinct_claim_count-previous_distinct_claim_count) / distinct_claim_count) * 100 
            else distinct_claim_count
    end as distinct_claim_percentage_change
from previous_service_category_1_claim_count

 union all

 select 
    year_month
    , service_category_2 as service_category
    , distinct_claim_count
    , distinct_claim_count-previous_distinct_claim_count as distinct_claim_count_change
    , case
        when distinct_claim_count <> 0 then ((distinct_claim_count-previous_distinct_claim_count) / distinct_claim_count) * 100 
            else distinct_claim_count
    end as distinct_claim_percentage_change
from previous_service_category_2_claim_count