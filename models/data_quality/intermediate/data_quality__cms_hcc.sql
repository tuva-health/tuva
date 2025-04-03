{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
select max(case when factor_type = 'Demographic' then 1 else 0 end) demographic_factor
,max(case when factor_type = 'Disease' then 1 else 0 end) as disease_factor
from {{ ref('cms_hcc__patient_risk_factors') }}
)

select 'missing cms-hcc demographic factor' as data_quality_check
,case when demographic_factor = 0 then 1 else 0 end as result_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte 

union all

select 'missing cms-hcc disease factor' as data_quality_check
,case when disease_factor = 0 then 1 else 0 end as result_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte 

