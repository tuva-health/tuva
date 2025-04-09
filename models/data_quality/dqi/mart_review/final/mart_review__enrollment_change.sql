{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with rankedmonths as (
    select
        person_id
        , year_month
        , data_source
        , lag(year_month_date, 1) over (partition by person_id, data_source
order by year_month_date) as prev_year_month
        , lead(year_month_date, 1) over (partition by person_id, data_source
order by year_month_date) as next_year_month
        , year_month_date
    from {{ ref('mart_review__stg_member_month') }}
)
, changes as (
 select
    person_id
    , data_source
    , year_month_date as change_month
    , case
        when prev_year_month is null
            or {{ dateadd('month', -1, 'year_month_date') }} != prev_year_month
        then 'added'
    end as change_type
from rankedmonths
union all
select
    person_id
    , data_source
    , {{ dateadd('month', 1, 'year_month_date') }} as change_month
    , case
        when next_year_month is null
            or {{ dateadd('month', 1, 'year_month_date') }} != next_year_month
        then 'removed'
    end as change_type
from rankedmonths

)
, final as (
    select
       {{ concat_custom(["person_id", "'|'", "change_month"]) }} as membermonthkey
        , data_source
        , person_id
        , change_month
        , change_type
    from changes
    where change_type is not null
)
, result as (
    select
        data_source
        , change_month
        , change_type
        , count(*) as member_count
    from final
    group by data_source
    , change_month
    , change_type
)


select * , '{{ var('tuva_last_run') }}' as tuva_last_run
from result
