{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select sum(case when numerator_sum = 0 then 1 else 0 end) as numerator_zero_count
,sum(case when denominator_sum = 0 then 1 else 0 end) as denominator_zero_count
from {{ ref('quality_measures__summary_counts') }} 
)

,final as (
select 'quality measure numerator zero' as data_quality_check
,numerator_zero_count as result_count
from cte 

union all 

select 'quality measure denominator zero' as data_quality_check
,denominator_zero_count as result_count
from cte
)

select *
, '{{ var('tuva_last_run') }}' as tuva_last_run
from final