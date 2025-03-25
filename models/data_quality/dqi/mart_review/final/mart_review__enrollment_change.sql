{{ config(
     enabled = var('claims_enabled',var('tuva_marts_enabled',False))
 | as_bool
   )
}}

with RANKEDMONTHS as (
    select
        PERSON_ID
        , YEAR_MONTH
        , DATA_SOURCE
        , lag(YEAR_MONTH_DATE, 1) over (partition by PERSON_ID, DATA_SOURCE
order by YEAR_MONTH_DATE) as PREV_YEAR_MONTH
        , lead(YEAR_MONTH_DATE, 1) over (partition by PERSON_ID, DATA_SOURCE
order by YEAR_MONTH_DATE) as NEXT_YEAR_MONTH
        , YEAR_MONTH_DATE
    from {{ ref('mart_review__stg_member_month') }}
)
, CHANGES as (
 select
    PERSON_ID
    , DATA_SOURCE
    , YEAR_MONTH_DATE as CHANGE_MONTH
    , case
        when PREV_YEAR_MONTH is null
            or {{ dateadd('month', -1, 'year_month_date') }} != PREV_YEAR_MONTH
        then 'added'
    end as CHANGE_TYPE
from RANKEDMONTHS
union all
select
    PERSON_ID
    , DATA_SOURCE,
    {{ dateadd('month', 1, 'year_month_date') }} as CHANGE_MONTH
    , case
        when NEXT_YEAR_MONTH is null
            or {{ dateadd('month', 1, 'year_month_date') }} != NEXT_YEAR_MONTH
        then 'removed'
    end as CHANGE_TYPE
from RANKEDMONTHS

)
, FINAL as (
    select
       {{ concat_custom(["person_id", "'|'", "change_month"]) }} as MEMBERMONTHKEY
        , DATA_SOURCE
        , PERSON_ID
        , CHANGE_MONTH
        , CHANGE_TYPE
    from CHANGES
    where CHANGE_TYPE is not null
)
, RESULT as (
    select
        DATA_SOURCE
        , CHANGE_MONTH
        , CHANGE_TYPE
        , count(*) as MEMBER_COUNT
    from FINAL
    group by DATA_SOURCE
    , CHANGE_MONTH
    , CHANGE_TYPE
)


select * , '{{ var('tuva_last_run') }}' as TUVA_LAST_RUN
from RESULT
