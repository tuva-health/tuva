{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with trend_by_medical_claim_type as(
    select
        cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , claim_type
        , sum(paid_amount) as total_paid_amount
        , sum(allowed_amount) as total_allowed_amount
        , sum(charge_amount) as total_charge_amount
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
        , claim_type
)
, trend_by_pharmacy_claim_type as(
    select
        cast({{ date_part("year", "dispensing_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "dispensing_date") }} as {{ dbt.type_string() }}),2) as year_month
        , cast('pharmacy' as {{ dbt.type_string() }}) as claim_type
        , sum(paid_amount) as total_paid_amount
        , sum(allowed_amount) as total_allowed_amount
        , 0 as total_charge_amount
    from {{ ref('core__pharmacy_claim') }}
    group by 
        year_month
)
, trend_with_previous_medical_sum as(
    select 
        year_month
        , claim_type
        , total_paid_amount
        , lag(total_paid_amount) over (partition by claim_type order by year_month) as previous_total_paid_amount
        , total_allowed_amount
        , lag(total_allowed_amount) over (partition by claim_type order by year_month) as previous_total_allowed_amount
        , total_charge_amount
        , lag(total_charge_amount) over (partition by claim_type order by year_month) as previous_total_charge_amount
    from trend_by_medical_claim_type
)
, trend_with_previous_pharmacy_sum as(
    select 
        year_month
        , claim_type
        , total_paid_amount
        , lag(total_paid_amount) over (partition by claim_type order by year_month) as previous_total_paid_amount
        , total_allowed_amount
        , lag(total_allowed_amount) over (partition by claim_type order by year_month) as previous_total_allowed_amount
        , total_charge_amount
        , lag(total_charge_amount) over (partition by claim_type order by year_month) as previous_total_charge_amount
    from trend_by_pharmacy_claim_type
)
select
    year_month
    , claim_type
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
from trend_with_previous_medical_sum

union all 

select
    year_month
    , claim_type
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
from trend_with_previous_pharmacy_sum