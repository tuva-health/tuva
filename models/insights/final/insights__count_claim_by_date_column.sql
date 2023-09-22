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


select * from
    (select
        year_month
        , date_field
        , distinct_count
        from date_stage
        ) s
        pivot
        (max(distinct_count) for date_field in ('claim_start_date','claim_end_date','admission_date'
        ,'discharge_date','medical paid_date','dispensing_date','pharmacy paid_date','member_months')
    )piv
