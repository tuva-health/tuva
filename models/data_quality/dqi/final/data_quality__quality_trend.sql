{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}


with cte as (
select {{ try_to_cast_date('d.source_date' ) }} as source_date_type
    , summary_sk
    , SUM(case when bucket_name = 'valid' then 1 else 0 end) as valid_num
    , SUM(case when bucket_name <> 'null' then 1 else 0 end) as fill_num
    , COUNT(drill_down_value) as denom
from {{ ref('data_quality__data_quality_detail') }} as d
group by
    {{ try_to_cast_date('d.source_date') }}
    , summary_sk

)

select
      c.first_day_of_month
    , summary_sk
    , SUM(valid_num) as valid_num
    , SUM(fill_num) as fill_num
    , SUM(denom) as denom
from cte
left outer join {{ ref('reference_data__calendar') }} as c on cte.source_date_type = c.full_date
group by
      c.first_day_of_month
    , summary_sk
