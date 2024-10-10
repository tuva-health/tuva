{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with eligibility_spans as(
    select distinct
        {{ dbt.concat([
            "member_id",
            "'-'",
            "enrollment_start_date",
            "'-'",
            "enrollment_end_date",
            "'-'",
            "payer",
            "'-'",
            quote_column('plan'),
        ]) }} as eligibility_span_id
        , enrollment_start_date
        , enrollment_end_date
    from {{ ref('eligibility') }}
)

, missing_start_date as (
    select
        'Missing enrollment_start_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date is null
),

missing_end_date as (
    select
        'Missing enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_end_date is null
),

invalid_start_date as (
    select
        'Enrollment_start_date populated with something other than a date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date is not null
    and not try_cast(enrollment_start_date as date) is not null
),

invalid_end_date as (
    select
        'Enrollment_end_date populated with something other than a date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_end_date is not null
    and not try_cast(enrollment_end_date as date) is not null
),

start_after_end as (
    select
        'Enrollment_start_date after enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date > enrollment_end_date
),

future_end_date as (
    select
        'Future enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_end_date > current_date
),

nonsensical_dates as (
    select
        'Nonsensical dates' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date < '1900-01-01'
    or enrollment_end_date < '1900-01-01'
    or enrollment_start_date > '2100-01-01'
)

select * from missing_start_date
union all
select * from missing_end_date
union all
select * from invalid_start_date
union all
select * from invalid_end_date
union all
select * from start_after_end
union all
select * from future_end_date
union all
select * from nonsensical_dates