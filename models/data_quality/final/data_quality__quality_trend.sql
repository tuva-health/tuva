{{ config(
     enabled = var('claims_enabled',var('clinical_enabled',False))
   )
}}

with cte as (
select {{ try_to_cast_date('substring(d.source_date,1,10)' ) }} as source_date_type
    ,summary_sk
    ,SUM(CASE WHEN BUCKET_NAME = 'valid' THEN 1 ELSE 0 END) as VALID_NUM
    ,SUM(CASE WHEN BUCKET_NAME <> 'null' THEN 1 ELSE 0 END) as FILL_NUM
    ,COUNT(DRILL_DOWN_VALUE) as DENOM
from {{ ref('data_quality__data_quality_detail') }} d
group by
    {{ try_to_cast_date('substring(d.source_date,1,10)' ) }}
    ,summary_sk

)

select
      c.first_day_of_month
    , summary_sk
    , sum(VALID_NUM) as VALID_NUM
    , sum(FILL_NUM) as FILL_NUM
    , sum(DENOM)  as DENOM
    , '{{ var('tuva_last_run')}}' as tuva_last_run
from cte
left join {{ ref('reference_data__calendar') }} c on cte.source_date_type = c.full_date
group by
      c.first_day_of_month
    , summary_sk
    , '{{ var('tuva_last_run')}}'
