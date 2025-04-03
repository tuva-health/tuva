{{ config(
     enabled = (var('enable_legacy_data_quality', False) and var('claims_enabled', var('tuva_marts_enabled', False))) | as_bool
)}}

with cte as (
    select count(*) as result_count
    ,'inpatient encounter missing professional claim' as data_quality_check
    from {{ ref('core__encounter') }}
    where encounter_type = 'acute inpatient'
    and prof_claim_count = 0 
)


select data_quality_check
,result_count
  , '{{ var('tuva_last_run') }}' as tuva_last_run
from cte

