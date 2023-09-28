{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}


with trend_by_service_category_1 as(
    select
        cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , 'service_category_1' as service_category_type
        , service_category_1 as service_category
        , sum(paid_amount) as total_paid_amount
        , sum(allowed_amount) as total_allowed_amount
        , sum(charge_amount) as total_charge_amount
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
        , service_category_1
)
, trend_by_service_category_2 as(
    select
        cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , 'service_category_2' as service_category_type
        , service_category_2 as service_category
        , sum(paid_amount) as total_paid_amount
        , sum(allowed_amount) as total_allowed_amount
        , sum(charge_amount) as total_charge_amount
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
        , service_category_2
)
, trend_with_previous_service_category_1_sum as(
    select 
        year_month
        , service_category_type
        , service_category
        , total_paid_amount
        , lag(total_paid_amount) over (partition by service_category order by year_month) as previous_total_paid_amount
        , total_allowed_amount
        , lag(total_allowed_amount) over (partition by service_category order by year_month) as previous_total_allowed_amount
        , total_charge_amount
        , lag(total_charge_amount) over (partition by service_category order by year_month) as previous_total_charge_amount
    from trend_by_service_category_1
)
, trend_with_previous_service_category_2_sum as(
    select 
        year_month
        , service_category_type
        , service_category
        , total_paid_amount
        , lag(total_paid_amount) over (partition by service_category order by year_month) as previous_total_paid_amount
        , total_allowed_amount
        , lag(total_allowed_amount) over (partition by service_category order by year_month) as previous_total_allowed_amount
        , total_charge_amount
        , lag(total_charge_amount) over (partition by service_category order by year_month) as previous_total_charge_amount
    from trend_by_service_category_2
)
select
    year_month
    , service_category_type
    , service_category
    , total_paid_amount
    , total_paid_amount - previous_total_paid_amount as total_paid_amount_change
    , case 
        when total_paid_amount <> 0 then ((total_paid_amount - previous_total_paid_amount)/total_paid_amount)*100
            else total_paid_amount
        end as total_paid_amount_percent_change
    , total_allowed_amount
    , total_allowed_amount - previous_total_allowed_amount as total_allowed_amount_change
    , case 
        when total_allowed_amount <> 0 then ((total_allowed_amount - previous_total_allowed_amount)/total_allowed_amount)*100
            else total_allowed_amount
        end as total_allowed_amount_percent_change
    , total_charge_amount
    , total_charge_amount - previous_total_charge_amount as total_charge_amount_change
    , case 
        when total_charge_amount <> 0 then ((total_charge_amount - previous_total_charge_amount)/total_charge_amount)*100
            else total_charge_amount
        end as total_charge_amount_percent_change
from trend_with_previous_service_category_1_sum

union all 

select
    year_month
    , service_category_type
    , service_category
    , total_paid_amount
    , total_paid_amount - previous_total_paid_amount as total_paid_amount_change
    , case 
        when total_paid_amount <> 0 then ((total_paid_amount - previous_total_paid_amount)/total_paid_amount)*100
            else total_paid_amount
        end as total_paid_amount_percent_change
    , total_allowed_amount
    , total_allowed_amount - previous_total_allowed_amount as total_allowed_amount_change
    , case 
        when total_allowed_amount <> 0 then ((total_allowed_amount - previous_total_allowed_amount)/total_allowed_amount)*100
            else total_allowed_amount
        end as total_allowed_amount_percent_change
    , total_charge_amount
    , total_charge_amount - previous_total_charge_amount as total_charge_amount_change
    , case 
        when total_charge_amount <> 0 then ((total_charge_amount - previous_total_charge_amount)/total_charge_amount)*100
            else total_charge_amount
        end as total_charge_amount_percent_change
from trend_with_previous_service_category_2_sum