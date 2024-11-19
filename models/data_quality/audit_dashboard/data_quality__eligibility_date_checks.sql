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
        , birth_date
        , death_date
    from {{ ref('eligibility') }}
)

, date_issues as (
    select
        eligibility_span_id,
        max(case when birth_date > death_date then 1 else 0 end) as birth_after_death,
        max(case when birth_date > enrollment_start_date then 1 else 0 end) as birth_after_enrollment_start,
        max(case when birth_date > enrollment_end_date then 1 else 0 end) as birth_after_enrollment_end,
        max(case when enrollment_start_date > enrollment_end_date then 1 else 0 end) as enrollment_start_after_end,
        max(case when enrollment_start_date > current_date() then 1 else 0 end) as future_enrollment_start,
        max(case when birth_date > current_date() then 1 else 0 end) as future_birth,
        max(case when death_date > current_date() then 1 else 0 end) as future_death
    from eligibility_spans
    group by
        eligibility_span_id

)

,final as (
select 'birth_date after death_date' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where birth_after_death > 1

union all

select 'birth_date after enrollment_start_date' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where birth_after_enrollment_start > 1

union all

select 'birth_date after enrollment_end_date' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where birth_after_enrollment_end > 1

union all

select 'enrollment_start_date after enrollment_end_date' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where enrollment_start_after_end > 1

union all

select 'enrollment_end_date in the future' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where future_enrollment_start > 1

union all

select 'birth_date in the future' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where future_birth > 1

union all

select 'death_date in the future' as data_quality_check,
       count(distinct eligibility_span_id) as result_count
from date_issues
where future_death > 1


)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final