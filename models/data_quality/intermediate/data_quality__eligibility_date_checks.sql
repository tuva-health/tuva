{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with eligibility_spans as(
    select distinct
        {{ concat_custom([
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
    from {{ ref('input_layer__eligibility') }}
)

, missing_start_date as (
    select
        'Missing enrollment_start_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date is null
)

, missing_end_date as (
    select
        'Missing enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_end_date is null
)

, invalid_start_date as (
    select
        'Enrollment_start_date populated with something other than a date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date is not null
    and not {{ try_to_cast_date('enrollment_start_date') }} is not null
)

, invalid_end_date as (
    select
        'Enrollment_end_date populated with something other than a date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_end_date is not null
    and not {{ try_to_cast_date('enrollment_end_date') }} is not null
)

, start_after_end as (
    select
        'Enrollment_start_date after enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date > enrollment_end_date
)

, future_end_date as (
    select
        'Future enrollment_end_date' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    {% if target.type == 'fabric' %}
        where enrollment_end_date > GETDATE()
    {% else %}
        where enrollment_end_date > current_date
    {% endif %}

)

, nonsensical_dates as (
    select
        'Nonsensical dates' as data_quality_check,
        count(*) as result_count
    from eligibility_spans
    where enrollment_start_date < {{ dbt.cast("'1900-01-01'", api.Column.translate_type('date')) }}
    or enrollment_end_date < {{ dbt.cast("'1900-01-01'", api.Column.translate_type('date')) }}
    or enrollment_start_date > {{ dbt.cast("'2100-01-01'", api.Column.translate_type('date')) }}
)

select *, '{{ var('tuva_last_run')}}' as tuva_last_run from missing_start_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from missing_end_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_start_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from invalid_end_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from start_after_end
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from future_end_date
union all
select *, '{{ var('tuva_last_run')}}' as tuva_last_run from nonsensical_dates
