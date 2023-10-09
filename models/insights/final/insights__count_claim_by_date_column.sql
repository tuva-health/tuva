{{ config(
     enabled = var('insights_enabled',var('claims_enabled',var('tuva_marts_enabled',False)))
   )
}}

with date_stage as(

    select
        'claim_start_date' as date_field
        , cast({{ date_part("year", "claim_start_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_start_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month

    union all

    select
        'claim_end_date' as date_field
        , cast({{ date_part("year", "claim_end_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "claim_end_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month
    union all

    select
        'admission_date' as date_field
        , cast({{ date_part("year", "admission_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "admission_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month

    union all

    select
        'discharge_date' as date_field
        , cast({{ date_part("year", "discharge_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "discharge_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month

    union all

    select
        'medical paid_date' as date_field
        , cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__medical_claim') }}
    group by 
        year_month

    union all

    select
        'dispensing_date' as date_field
        , cast({{ date_part("year", "dispensing_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "dispensing_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__pharmacy_claim') }}
    group by 
        year_month

    union all

    select
        'pharmacy paid_date' as date_field
        , cast({{ date_part("year", "paid_date") }} as {{ dbt.type_string() }}) || right('0'||cast({{ date_part("month", "paid_date") }} as {{ dbt.type_string() }}),2) as year_month
        , count(distinct claim_id) as distinct_count
    from {{ ref('core__pharmacy_claim') }}
    group by 
        year_month

    union all 

    select
        'member_months' as date_field
        , year_month
        , count(*) as distinct_count
    from {{ ref('financial_pmpm__member_months') }}
    group by
        year_month
)


, all_date_range as (
    select distinct 
        replace(cal.year_month,'-','') as year_month
    from {{ ref('terminology__calendar') }} cal
    where (cal.year_month >= (select min(year_month) from date_stage)
    and cal.year_month <= (select max(year_month) from date_stage))
    
)

select
    cast(all_date.year_month as {{ dbt.type_int() }} ) as year_month
    , member_months.distinct_count as member_months
    , claim_start.distinct_count as claim_start_date
    , claim_end.distinct_count as claim_end_date
    , admission_date.distinct_count as admission_date
    , discharge_date.distinct_count as discharge_date
    , med_paid_date.distinct_count as medical_paid_date
    , dispensing_date.distinct_count as dispensing_date
    , pharm_paid_date.distinct_count as pharmacy_paid_date
from all_date_range all_date
left join date_stage member_months
    on all_date.year_month = member_months.year_month
    and member_months.date_field = 'member_months'
left join date_stage claim_start
    on all_date.year_month = claim_start.year_month
    and claim_start.date_field = 'claim_start_date'
left join date_stage claim_end
    on all_date.year_month = claim_end.year_month
    and claim_end.date_field = 'claim_end_date'
left join date_stage admission_date
    on all_date.year_month = admission_date.year_month
    and admission_date.date_field = 'admission_date'
left join date_stage discharge_date
    on all_date.year_month = discharge_date.year_month
    and discharge_date.date_field = 'discharge_date'
left join date_stage med_paid_date
    on all_date.year_month = med_paid_date.year_month
    and med_paid_date.date_field = 'medical paid_date'
left join date_stage dispensing_date
    on all_date.year_month = dispensing_date.year_month
    and dispensing_date.date_field = 'dispensing_date'
left join date_stage pharm_paid_date
    on all_date.year_month = pharm_paid_date.year_month
    and pharm_paid_date.date_field = 'pharmacy paid_date'

