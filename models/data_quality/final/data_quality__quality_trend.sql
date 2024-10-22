{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}


with cte as (
select {{ try_to_cast_date('d.source_date' ) }} as source_date_type
    ,summary_sk
    ,SUM(CASE WHEN bucket_name = 'valid' THEN 1 ELSE 0 END) as valid_num
    ,SUM(CASE WHEN bucket_name <> 'null' THEN 1 ELSE 0 END) as fill_num
    ,COUNT(drill_down_value) as denom
from {{ ref('data_quality__data_quality_detail') }} d
group by
    {{ try_to_cast_date('d.source_date') }}
    ,summary_sk

)

select
      c.first_day_of_month
    , summary_sk
    , sum(valid_num) as valid_num
    , sum(fill_num) as fill_num
    , sum(denom)  as denom
from cte
left join {{ ref('reference_data__calendar') }} c on cte.source_date_type = c.full_date
group by
      c.first_day_of_month
    , summary_sk

